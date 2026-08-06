package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mohgh/nexus/internal/api"
	"github.com/mohgh/nexus/internal/api/middleware"
	"github.com/mohgh/nexus/internal/audit"
	"github.com/mohgh/nexus/internal/auth"
	"github.com/mohgh/nexus/internal/billing/outbox"
	billingtemporal "github.com/mohgh/nexus/internal/billing/temporal"
	"github.com/mohgh/nexus/internal/chaos"
	"github.com/mohgh/nexus/internal/config"
	"github.com/mohgh/nexus/internal/consent"
	"github.com/mohgh/nexus/internal/domain"
	"github.com/mohgh/nexus/internal/election"
	"github.com/mohgh/nexus/internal/eventstore"
	"github.com/mohgh/nexus/internal/pii"
	"github.com/mohgh/nexus/internal/projections"
	"github.com/mohgh/nexus/internal/gdpr"
	"github.com/mohgh/nexus/internal/idgen"
	"github.com/mohgh/nexus/internal/ratelimit"
	"github.com/mohgh/nexus/internal/resilience"
	chstore "github.com/mohgh/nexus/internal/storage/clickhouse"
	pgstore "github.com/mohgh/nexus/internal/storage/postgres"
	redisstore "github.com/mohgh/nexus/internal/storage/redis"
	"github.com/mohgh/nexus/internal/storage/shard"
	"github.com/mohgh/nexus/internal/stream"
	"go.uber.org/zap"
)

// Nexus — Data-Intensive SaaS Platform
//
// This binary is the running project for the DDIA course.
// Each chapter wires in new dependencies below — the binary grows in place.
//
// Dependencies are connected optionally: if a service (Redis, ClickHouse,
// Kafka, Temporal) is not running, that feature is skipped with a warning.
// Only PostgreSQL is required (from Ch03 onwards).
//
//	Chapter 01: In-memory tenant store
//	Chapter 02: Prometheus metrics
//	Chapter 03: PostgreSQL — tenant + event repositories
//	Chapter 04: Redis cache, Elasticsearch, ClickHouse
//	Chapter 05: Kafka producer, Protobuf encoding
//	Chapter 06: Read replica pool with LSN-aware routing
//	Chapter 07: Shard router
//	Chapter 08: Billing service, Temporal worker
//	Chapter 09: Circuit breakers, chaos endpoint
//	Chapter 10: Leader election, ULID generator
//	Chapter 11: Batch aggregator (separate binary: cmd/batch-aggregator)
//	Chapter 12: Stream processor (separate binary: cmd/stream-processor)
//	Chapter 13: Event store, CQRS projections
//	Chapter 14: Audit log, consent gate, PII scanner
func main() {
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialise logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	ctx := context.Background()

	// ─── Chapter 03: PostgreSQL (required) ────────────────────────────────────
	// This is the only required dependency. Without Postgres, nothing works.
	// Run: make docker-up && make migrate-up
	pool, err := pgstore.NewPool(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Fatal("postgres: connect failed — run 'make docker-up && make migrate-up'",
			zap.Error(err))
	}
	defer pool.Close()

	// ─── Chapter 06: Read replica + LSN-aware routing ────────────────────────
	// The replica pool wraps the primary above. With no replica configured
	// (POSTGRES_REPLICA_DSN empty) it runs in primary-only mode and the
	// repositories below behave identically to single-node Postgres.
	// Start: docker compose --profile replication up -d
	replicaPool, err := pgstore.NewReplicaPool(ctx, pool, cfg.PostgresReplicaDSN)
	if err != nil {
		logger.Warn("replica pool: replica DSN unreachable — primary-only mode",
			zap.Error(err))
		// Empty replica DSN cannot fail; safe to ignore the error here.
		replicaPool, _ = pgstore.NewReplicaPool(ctx, pool, "")
	}
	defer replicaPool.Close()

	var tenants domain.TenantRepository = pgstore.NewTenantRepository(replicaPool)

	// ─── Authentication: DB-backed API keys + bootstrap admin key ────────────
	// Every /api/v1 route requires a valid API key. The store uses the
	// PRIMARY pool directly so a freshly-minted or just-revoked key takes
	// effect immediately — no replica lag on the auth path.
	//
	// NEXUS_BOOTSTRAP_ADMIN_KEY (if set) is hashed and upserted as an
	// admin-scoped key so an operator has a credential to mint per-tenant
	// keys via POST /api/v1/admin/api-keys. The raw value is never logged.
	apiKeys := pgstore.NewAPIKeyRepository(pool.Pool)
	if cfg.BootstrapAdminKey != "" {
		if err := apiKeys.EnsureAdminKey(ctx,
			auth.HashKey(cfg.BootstrapAdminKey), auth.PrefixOf(cfg.BootstrapAdminKey)); err != nil {
			logger.Fatal("bootstrap admin key: failed to register", zap.Error(err))
		}
		logger.Info("bootstrap admin key registered — mint tenant keys via POST /api/v1/admin/api-keys")
	} else if !strings.EqualFold(cfg.Env, "production") {
		logger.Warn("NEXUS_BOOTSTRAP_ADMIN_KEY not set — admin routes reject every request until an admin key exists in the database")
	}
	authenticator := middleware.NewAuthenticator(apiKeys)

	// ─── Per-tenant rate limiting ────────────────────────────────────────────
	// On by default. Buckets are sized by each tenant's plan (free/pro/
	// enterprise); admin keys get a generous ceiling. In-process token
	// bucket — see internal/ratelimit for the multi-instance caveat.
	var rateLimiter *ratelimit.MemoryLimiter
	if cfg.RateLimitEnabled {
		rateLimiter = ratelimit.NewMemoryLimiter()
		defer rateLimiter.Close()
		logger.Info("rate limiting enabled (per-tenant, plan-sized)")
	} else {
		logger.Warn("rate limiting DISABLED (RATE_LIMIT_ENABLED=false)")
	}

	// ─── Chapter 09: Circuit breakers + chaos profile ────────────────────────
	// The Server is constructed FIRST so the Prometheus registry
	// it owns is available for resilience.InstallMetrics below.
	// Without that ordering, breakers created by
	// NewResilientEventRepository would record no transitions.
	breakerReg := resilience.NewRegistry()
	srv := api.NewServer(cfg, logger, tenants).
		WithBreakers(breakerReg).
		WithReplicaPool(replicaPool).
		WithAuth(authenticator).
		WithAPIKeys(apiKeys)
	// Attach the rate limiter only when constructed: passing a nil
	// *MemoryLimiter through the Limiter interface would be a non-nil
	// interface value and defeat the nil check in Router().
	if rateLimiter != nil {
		srv.WithRateLimiter(rateLimiter)
	}

	// Hook breaker state transitions onto the shared registry so
	// the closed→open transition shows up on dashboards alongside
	// the current state from /api/v1/circuit-breakers.
	resilience.InstallMetrics(srv.Metrics())

	// Chaos profile threads in front of the resilience wrapper so
	// the slow-vs-dead demo works: a 15s injected delay (longer
	// than the inner 5s repo timeout) trips the breaker; a 4s delay
	// (within the timeout) does not.
	chaosProfile := chaos.New()
	events := chaos.NewEventRepository(chaosProfile,
		resilience.NewResilientEventRepository(
			pgstore.NewEventRepository(replicaPool),
			breakerReg,
			logger,
		),
	)
	srv.WithEvents(events).WithChaos(chaosProfile)

	// FencedResource for the Ch09/Ch10 stale-leader write demo.
	// Endpoint at /api/v1/protected; pair with /api/v1/leader's
	// global_fencing_token for the canonical DDIA Ch9 sequence.
	srv.WithFencedResource(election.NewFencedResource())

	// ─── Chapter 04: Redis cache + Chapter 10 elector (both optional) ────────
	// We construct the elector here when Redis is available, but don't yet
	// attach the leader-side work — that happens later, once the outbox
	// worker has its dependencies wired (billing repo + Kafka publisher).
	var elector *election.Elector
	cache, err := redisstore.NewCache(ctx, cfg.RedisDSN)
	if err != nil {
		logger.Warn("redis: not available — cache and leader election disabled",
			zap.Error(err))
	} else {
		defer cache.Close()
		srv.WithCache(cache)

		// Ch04: wrap the tenant repo in a read-through cache and
		// swap it into the server. Hit/miss/error counts land on
		// the metrics registry we just exposed via Metrics().
		cachedTenants := redisstore.NewCachedTenantRepository(
			tenants, cache, srv.Metrics(), 30*time.Second,
		)
		srv.WithTenants(cachedTenants)

		idGen := idgen.NewGenerator()
		nodeID := idGen.New()
		logger.Info("node started", zap.String("node_id", nodeID))

		elector = election.NewElector(cache.Client(), "outbox-worker", nodeID)
		srv.WithLeader(elector)
	}

	// ─── Chapter 04: ClickHouse OLAP (optional) ──────────────────────────────
	// Start: docker compose --profile olap up -d
	if cfg.ClickHouseDSN != "" {
		ch, err := chstore.NewClient(ctx, cfg.ClickHouseDSN)
		if err != nil {
			logger.Warn("clickhouse: not available — OLAP queries disabled",
				zap.Error(err))
		} else {
			defer ch.Close()
			srv.WithClickHouse(ch)
			// EventRepository drives the /daily-stats handler;
			// the bare client stays wired for /ready. WithMetrics
			// hooks the repo into OLAPQueryDuration so Grafana
			// can render the chapter's latency-comparison claim
			// without re-running cmd/storage-benchmark each time.
			srv.WithClickHouseEvents(
				chstore.NewEventRepository(ch).WithMetrics(srv.Metrics()),
			)
		}
	}

	// ─── Chapter 07: Shard router ─────────────────────────────────────────────
	shardRouter, err := shard.NewRouter(cfg.ShardCount)
	if err != nil {
		logger.Warn("shard router: init failed", zap.Error(err))
	} else {
		srv.WithShardRouter(shardRouter)

		// Opt-in true sharded mode: when SHARD_DSN_TEMPLATE is set,
		// open one Postgres pool per shard and wire the shard-aware
		// EventRepository as the canonical events store. Without
		// this, the router computes shard indices but every write
		// still lands on the single Postgres — the audit's specific
		// "routing math, not sharded data management" finding.
		if cfg.ShardDSNTemplate != "" {
			shardedPool, err := shard.NewShardedPool(ctx, shardRouter, cfg.ShardDSNTemplate)
			if err != nil {
				logger.Warn("shard pool: not available — staying in single-Postgres mode",
					zap.Error(err))
			} else {
				defer shardedPool.Close()
				shardedEvents := shard.NewEventRepository(shardedPool)
				// Replace the previously-wired single-Postgres
				// events repo with the sharded one. The resilience
				// wrapper is reapplied so circuit breakers still
				// guard the per-shard hot paths, AND chaos is
				// re-applied so the Ch09 fault-injection toggles
				// still affect event_create/list/search in
				// sharded mode. Forgetting the chaos wrap was the
				// bug the audit flagged: /api/v1/chaos kept
				// mutating state but the toggles silently
				// stopped affecting writes.
				srv.WithEvents(chaos.NewEventRepository(chaosProfile,
					resilience.NewResilientEventRepository(
						shardedEvents, breakerReg, logger,
					),
				))
				srv.WithShardAdmin(shardedEvents)
				logger.Info("shard pool: sharded events repository wired",
					zap.Int("shards", shardRouter.ShardCount()))
			}
		}
	}

	// ─── Chapter 08: Billing repo (always wired so the outbox worker has data) ─
	billingRepo := pgstore.NewBillingRepository(pool)

	// ─── Chapter 08: Temporal worker (optional — only the workflow path) ─────
	// Start: docker compose --profile workflows up -d
	temporalClient, err := billingtemporal.NewClient(cfg.TemporalHostPort)
	if err != nil {
		logger.Warn("temporal: not available — billing workflows disabled",
			zap.Error(err))
	} else {
		defer temporalClient.Close()

		billingWorker := billingtemporal.StartWorker(temporalClient, tenants, billingRepo, pool)
		if err := billingWorker.Start(); err != nil {
			logger.Warn("temporal worker: start failed — billing disabled",
				zap.Error(err))
		} else {
			defer billingWorker.Stop()
			srv.WithBilling(billingtemporal.NewStarter(temporalClient))
		}
	}

	// ─── Chapter 05: Kafka producer for analytics events (optional) ──────────
	// Start: docker compose --profile streaming up -d
	// The producer connects lazily — it won't fail here even if Kafka is down.
	// Publish errors are logged but don't fail the HTTP request (fire-and-forget).
	//
	// Wrapped in chaos.Publisher so the drop_publish toggle actually
	// affects something. The wrapper short-circuits publish when the
	// toggle is set — silent "success" — which produces the
	// asymmetric-partial-failure demo (DB write commits, downstream
	// Kafka consumer sees nothing). Without the wrapper, drop_publish
	// would be inert: the toggle would flip but Publish would still
	// go through unchanged.
	producer := stream.NewEventProducer(cfg.KafkaBrokers)
	defer producer.Close()
	srv.WithProducer(chaos.NewPublisher(chaosProfile, producer))

	// ─── Chapter 08: Transactional outbox worker ─────────────────────────────
	// Polls billing_records WHERE outbox_sent_at IS NULL and publishes each
	// to the nexus.billing Kafka topic. At-least-once delivery; downstream
	// consumers must dedupe (Ch12). Uses a separate KafkaPublisher because
	// billing payloads are JSON-shaped, not the Protobuf used for events.
	//
	// Singleton via leader election when Redis is available; otherwise
	// runs unconditionally. In a multi-instance deploy without Redis the
	// at-least-once contract still holds — you just get N× publish work.
	billingPublisher := outbox.NewKafkaPublisher(cfg.KafkaBrokers)
	defer billingPublisher.Close() //nolint:errcheck
	outboxWorker := outbox.New(billingRepo, billingPublisher, logger, outbox.Config{})
	if elector != nil {
		go elector.RunAsLeader(ctx, func(leaderCtx context.Context) {
			logger.Info("outbox worker: leader elected, starting")
			if err := outboxWorker.Run(leaderCtx); err != nil {
				logger.Error("outbox worker: exited with error", zap.Error(err))
			}
		})
	} else {
		go func() {
			if err := outboxWorker.Run(ctx); err != nil {
				logger.Error("outbox worker: exited with error", zap.Error(err))
			}
		}()
	}

	// ─── Chapter 13: event store + projection runner ─────────────────────────
	// EventRepository.Create now dual-writes events_store + events
	// in one tx, so the event log is always populated whenever an
	// event is ingested. The projection runner reads from events_store
	// and maintains tenant_event_counts (running totals) and
	// daily_event_counts (per-day rollups) as catch-up consumers.
	// Both projections persist their position in projection_positions
	// so a restart resumes; cmd/projection-rebuild resets them to 0
	// and replays.
	//
	// Sharded mode (Ch07): when SHARD_DSN_TEMPLATE is set, the
	// sharded events repository writes to each shard's own
	// events_store. The central pool's events_store is therefore
	// empty, and starting the central projection runner would
	// silently report healthy lag while ignoring every sharded
	// write. We skip the runner entirely and wire a 503 sentinel
	// on /api/v1/projections so the operator sees the degraded
	// state instead of a misleading "lag=0" response.
	if cfg.ShardDSNTemplate != "" {
		const reason = "projection runner is not shard-aware (Ch07 v2 follow-up); " +
			"in sharded mode each shard owns its own events_store. " +
			"See chapters/07-sharding/README.md."
		srv.WithProjectionsUnavailable(reason)
		logger.Warn("ch13: projection runner DISABLED in sharded mode",
			zap.String("reason", reason),
		)
	} else {
		eventStore := eventstore.NewStore(pool.Pool)

		tenantStatsProj := projections.NewTenantStatsProjection(pool.Pool, logger)
		dailyStatsProj := projections.NewDailyEventCountsProjection(pool.Pool, logger)
		projectionRunner := projections.NewRunner(eventStore,
			[]projections.Projection{tenantStatsProj, dailyStatsProj},
			logger, projections.Config{},
		)
		srv.WithProjectionRunner(projectionRunner)

		go func() {
			if err := projectionRunner.Run(ctx); err != nil {
				logger.Error("projection runner: exited with error", zap.Error(err))
			}
		}()
	}

	// ─── Chapter 13: idempotency cleanup goroutine ───────────────────────────
	// The Idempotency middleware itself is wired in router.go; here we
	// just keep the dedup table from growing unboundedly by sweeping
	// expired entries hourly.
	srv.WithIdempotencyPool(pool.Pool)
	go middleware.RunIdempotencyCleanup(ctx, pool.Pool, 0, logger)

	// ─── Chapter 14: Audit, consent, GDPR ─────────────────────────────────────
	auditLog := audit.NewLog(pool.Pool)
	consentStore := consent.NewStore(pool.Pool)
	gdprService := gdpr.NewService(pool.Pool, auditLog, consentStore, logger)
	srv.WithGDPR(gdprService, gdprService)
	// The consent checker drives the ConsentGate middleware on
	// tenant-scoped analytics routes — see WithConsentChecker for
	// the policy.
	srv.WithConsentChecker(consentStore)
	// PIIDetect middleware scrubs URLs/queries before they hit the
	// access log. The masker is the same regex-based detector used
	// by GDPR anonymisation and cmd/pii-scanner.
	srv.WithPIIMasker(pii.NewMasker())

	// ─── HTTP Server ──────────────────────────────────────────────────────────
	httpServer := &http.Server{
		Addr:         cfg.Addr,
		Handler:      srv.Router(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// ─── Start ────────────────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		logger.Info("Nexus listening",
			zap.String("addr", cfg.Addr),
			zap.String("env", cfg.Env),
		)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("server error", zap.Error(err))
		}
	}()

	// ─── Graceful shutdown ────────────────────────────────────────────────────
	<-quit
	logger.Info("shutting down...")

	shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutCtx); err != nil {
		logger.Error("forced shutdown", zap.Error(err))
	}
	logger.Info("stopped")
}
