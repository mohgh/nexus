package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mohgh/nexus/internal/api/handlers"
	nexusmw "github.com/mohgh/nexus/internal/api/middleware"
	"github.com/mohgh/nexus/internal/config"
	"github.com/mohgh/nexus/internal/domain"
	"github.com/mohgh/nexus/internal/metrics"
	"github.com/mohgh/nexus/internal/pii"
	"github.com/mohgh/nexus/internal/projections"
	"github.com/mohgh/nexus/internal/ratelimit"
	chstore "github.com/mohgh/nexus/internal/storage/clickhouse"
	redisstore "github.com/mohgh/nexus/internal/storage/redis"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Server holds every application-wide dependency and wires them to HTTP handlers.
// Each chapter adds new fields as new systems are introduced:
//
//	Ch02: metrics registry
//	Ch03: event repository
//	Ch04: redis cache, clickhouse client
//	Ch05: kafka producer
//	Ch08: billing service, temporal client
//	Ch09: circuit breaker registry
//	Ch10: leader elector, id generator
type Server struct {
	cfg     *config.Config
	logger  *zap.Logger
	tenants domain.TenantRepository

	// Ch02: Prometheus instruments and registry for the /metrics endpoint.
	metrics *metrics.Registry
	promReg *prometheus.Registry

	// Ch03: event ingestion + search. Nil until WithEvents is called.
	events domain.EventRepository

	// Ch04: read-through cache, OLAP client, OLAP repository.
	cache         *redisstore.Cache
	clickhouse    *chstore.Client
	clickhouseEvents handlers.DailyStatsQuerier

	// Ch05: Kafka producer for event streaming.
	producer handlers.Publisher

	// Ch06: replica pool for LSN-aware read routing and lag monitoring.
	// Combines lag reporting, routing inspection, and LSN recording —
	// all satisfied by *postgres.ReplicaPool.
	replicaPool replicaPoolInspector

	// Ch07: shard router — maps tenant IDs to shard indices.
	shardRouter handlers.ShardMapper

	// Ch07: cross-shard admin surface for distribution + load
	// endpoints. Nil when the server isn't running in sharded
	// mode; the admin endpoints aren't mounted in that case.
	shardAdmin handlers.ShardAdminer

	// Ch08: Temporal billing workflow starter.
	billing handlers.WorkflowStarter

	// Ch09: circuit breaker registry for observability.
	breakers handlers.BreakerInspector

	// Ch09: chaos profile (fault-injection toggles) and the
	// FencedResource that protects a write demo. Both nil when
	// the chapter's interactive demos aren't wired.
	chaos          handlers.ChaosProfile
	fencedResource handlers.FencedResource

	// Ch10: leader election inspector.
	leader handlers.LeaderInspector

	// Ch13: projection runner (drives read-models off the event store)
	// and the Postgres pool used by the idempotency middleware.
	projectionRunner *projections.Runner
	idempotencyPool  *pgxpool.Pool

	// Ch07: when set, /api/v1/projections returns 503 with this
	// reason rather than the central runner's (misleading) lag
	// report. Used in sharded mode where the central events_store
	// is empty.
	projectionsUnavailableReason string

	// Ch14: GDPR compliance — export, erasure, consent.
	gdprExporter DataExporterEraser
	gdprConsent  handlers.ConsentManager

	// Ch14: consent checker for the analytics-purpose gate on
	// tenant-scoped routes. Nil = no gating.
	consentChecker nexusmw.ConsentChecker

	// Ch14: PII masker used by the PIIDetect middleware to scan
	// request URLs and stamp a masked path into the context for
	// the metrics access log. Nil = PIIDetect is a no-op.
	piiMasker *pii.Masker

	// Production-hardening: API-key authentication. When set, the
	// /api/v1 routes are split into admin-scoped and tenant-scoped
	// groups, each gated by this authenticator. Nil leaves the API
	// open — used only by tests that exercise handlers directly; the
	// production binary always wires it in main.go.
	auth *nexusmw.Authenticator

	// Production-hardening: admin surface for minting/revoking API
	// keys. Mounted under the admin group when set.
	apiKeys handlers.APIKeyStore

	// Production-hardening: per-principal rate limiter. Applied inside
	// each authenticated route group (after auth, before idempotency)
	// when set. Nil disables rate limiting.
	rateLimiter ratelimit.Limiter
}

// WithRateLimiter wires the per-principal rate limiter. Applied after
// authentication so limits can be keyed on the tenant and sized by its plan.
func (s *Server) WithRateLimiter(l ratelimit.Limiter) *Server {
	s.rateLimiter = l
	return s
}

// WithAuth wires the API-key authenticator. When set, every /api/v1 route
// requires a valid key: admin-scoped routes demand an admin key, tenant
// routes derive their tenant from the key (never the body/URL), and
// {tenantID} routes reject any key whose tenant differs from the path.
func (s *Server) WithAuth(a *nexusmw.Authenticator) *Server {
	s.auth = a
	return s
}

// WithAPIKeys wires the admin key-management endpoints
// (POST/DELETE /api/v1/admin/api-keys).
func (s *Server) WithAPIKeys(store handlers.APIKeyStore) *Server {
	s.apiKeys = store
	return s
}

// WithPIIMasker wires the Ch14 PII masker. When set, PIIDetect runs
// on every request, scrubbing the URL/query before it's logged.
func (s *Server) WithPIIMasker(m *pii.Masker) *Server {
	s.piiMasker = m
	return s
}

// WithConsentChecker wires the Ch14 consent checker. When set, the
// tenant-scoped analytics routes (currently /tenants/{id}/live-stats)
// are wrapped in ConsentGate which 403s on explicit revoke.
func (s *Server) WithConsentChecker(c nexusmw.ConsentChecker) *Server {
	s.consentChecker = c
	return s
}

// WithProjectionRunner wires the Ch13 projection runner so the
// /api/v1/projections lag endpoint can render its state.
func (s *Server) WithProjectionRunner(r *projections.Runner) *Server {
	s.projectionRunner = r
	return s
}

// WithProjectionsUnavailable marks the /api/v1/projections route
// as 503 with the supplied reason. Used in Ch07 sharded mode where
// the central projection runner is not shard-aware and starting
// it against the (empty) central events_store would silently report
// healthy lag while ignoring every sharded write.
//
// When both this and WithProjectionRunner are set, the sentinel
// wins — operating in a "degraded" mode should be visible to the
// caller, not silently overridden.
func (s *Server) WithProjectionsUnavailable(reason string) *Server {
	s.projectionsUnavailableReason = reason
	return s
}

// WithIdempotencyPool wires the Postgres pool the Ch13 Idempotency
// middleware uses for the dedup table. With this set, the middleware
// is mounted globally (it short-circuits non-mutating requests and
// requests without an Idempotency-Key header).
func (s *Server) WithIdempotencyPool(pool *pgxpool.Pool) *Server {
	s.idempotencyPool = pool
	return s
}

// DataExporterEraser combines GDPR interfaces for convenience —
// export, erase, and the lighter-touch anonymise. The Ch14 service
// implements all three.
type DataExporterEraser interface {
	handlers.DataExporter
	handlers.DataEraser
	handlers.DataAnonymiser
}

// WithGDPR wires the GDPR service for export, erasure, anonymisation,
// and consent.
func (s *Server) WithGDPR(ee DataExporterEraser, cm handlers.ConsentManager) *Server {
	s.gdprExporter = ee
	s.gdprConsent = cm
	return s
}

// WithLeader wires the leader elector for the status endpoint.
func (s *Server) WithLeader(l handlers.LeaderInspector) *Server {
	s.leader = l
	return s
}

// WithShardRouter wires the shard router into the server.
func (s *Server) WithShardRouter(r handlers.ShardMapper) *Server {
	s.shardRouter = r
	return s
}

// WithShardAdmin wires the cross-shard admin surface. Set only when
// the server is running with actual per-shard pools — the
// distribution / load endpoints are mounted iff this is non-nil.
func (s *Server) WithShardAdmin(a handlers.ShardAdminer) *Server {
	s.shardAdmin = a
	return s
}

// WithBilling wires the Temporal billing workflow starter.
func (s *Server) WithBilling(b handlers.WorkflowStarter) *Server {
	s.billing = b
	return s
}

// WithBreakers wires the circuit breaker registry for the status endpoint.
func (s *Server) WithBreakers(b handlers.BreakerInspector) *Server {
	s.breakers = b
	return s
}

// WithChaos wires the Ch09 fault-injection profile. When set, the
// /api/v1/chaos endpoints are mounted; otherwise they're absent.
func (s *Server) WithChaos(p handlers.ChaosProfile) *Server {
	s.chaos = p
	return s
}

// WithFencedResource wires the Ch09/Ch10 protected-write demo.
// When set, the /api/v1/protected endpoints are mounted.
func (s *Server) WithFencedResource(r handlers.FencedResource) *Server {
	s.fencedResource = r
	return s
}

// WithProducer wires the Kafka event producer into the server.
func (s *Server) WithProducer(p handlers.Publisher) *Server {
	s.producer = p
	return s
}

// replicaPoolInspector is the union of replication interfaces the server
// needs from a Ch06 replica pool — lag reads for the monitor endpoint
// and routing introspection for the status endpoint. The RYOW middleware
// no longer needs an explicit dependency: it threads a per-request
// WriteLSNRecorder through context, which ReplicaPool.Exec populates.
// Both interfaces are satisfied by *postgres.ReplicaPool.
type replicaPoolInspector interface {
	handlers.LagReader
	handlers.RoutingInspector
}

// WithReplicaPool wires the replica pool for lag monitoring, routing
// inspection, and read-your-own-writes header threading.
// Ch06: start postgres-replica profile, then wire this.
func (s *Server) WithReplicaPool(r replicaPoolInspector) *Server {
	s.replicaPool = r
	return s
}

// WithEvents wires the event repository into the server.
func (s *Server) WithEvents(repo domain.EventRepository) *Server {
	s.events = repo
	return s
}

// WithTenants swaps the tenant repository — used by Ch04 to wrap
// the underlying repo in a read-through cache decorator after the
// Redis cache is wired. The decorator preserves the
// domain.TenantRepository contract; handlers see no difference.
func (s *Server) WithTenants(repo domain.TenantRepository) *Server {
	s.tenants = repo
	return s
}

// Metrics returns the Prometheus instrument registry the server
// uses, so the Ch04 cache wrapper (and any future instrumentation)
// can record per-cache counters against the same registry.
func (s *Server) Metrics() *metrics.Registry {
	return s.metrics
}

// WithCache wires the Redis cache into the server.
// Ch04: used for tenant response caching.
func (s *Server) WithCache(c *redisstore.Cache) *Server {
	s.cache = c
	return s
}

// WithClickHouse wires the ClickHouse client into the server.
// Ch04: used for OLAP query endpoints. The accompanying
// WithClickHouseEvents call provides the repository the handlers
// use; the bare client is also kept for /ready checks.
func (s *Server) WithClickHouse(c *chstore.Client) *Server {
	s.clickhouse = c
	return s
}

// WithClickHouseEvents wires the ClickHouse event repository for
// the Ch04 OLAP endpoints (currently /tenants/{id}/daily-stats).
func (s *Server) WithClickHouseEvents(q handlers.DailyStatsQuerier) *Server {
	s.clickhouseEvents = q
	return s
}

// NewServer constructs a Server. Wire dependencies here in main.go and pass
// them in — no global state, no init() magic.
func NewServer(cfg *config.Config, logger *zap.Logger, tenants domain.TenantRepository) *Server {
	// Ch02: isolated Prometheus registry — tests get a clean slate.
	promReg := prometheus.NewRegistry()
	promReg.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)

	return &Server{
		cfg:     cfg,
		logger:  logger,
		tenants: tenants,
		metrics: metrics.New(promReg),
		promReg: promReg,
	}
}

// Router builds and returns the HTTP handler tree.
// Called once at startup. Routes are added chapter by chapter.
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()

	// ─── Middleware ───────────────────────────────────────────────────────────
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(exposeRequestID) // copy request ID to response header for structured errors
	// Appendix A (JS SDK): browser requests need CORS or they're
	// rejected before reaching any handler. No-op when no origins
	// are configured.
	r.Use(nexusmw.CORS(nexusmw.CORSConfig{AllowedOrigins: s.cfg.CORSAllowedOrigins}))
	// Ch14: PII detection on URL+query. Runs BEFORE the metrics
	// middleware so the masked path is in ctx by the time the
	// access log is written. No-op if no masker is wired.
	r.Use(nexusmw.PIIDetect(s.piiMasker, s.logger))
	r.Use(s.metricsMiddleware())                // Ch02: records duration + status in Prometheus
	r.Use(middleware.Timeout(10 * time.Second)) // Ch09: request-level timeout

	// Ch06: read-your-own-writes header threading. Reads X-Nexus-Min-LSN
	// from incoming requests, attaches a per-request WriteLSNRecorder
	// (populated by ReplicaPool.Exec on each successful write), and
	// stamps X-Nexus-Write-LSN on responses to mutating requests that
	// actually advanced the recorder. Always-on: it's harmless when no
	// replica pool is wired (the recorder simply never gets populated).
	r.Use(handlers.RYOWMiddleware())

	// Ch13: idempotency-key dedup. Built here but applied per route group
	// AFTER authentication (see below) — the middleware namespaces cached
	// keys by the authenticated principal, so it must run with the principal
	// already in context. Nil when the pool isn't wired (tests).
	var idempotency func(http.Handler) http.Handler
	if s.idempotencyPool != nil {
		idempotency = nexusmw.Idempotency(nexusmw.IdempotencyConfig{
			Pool:   s.idempotencyPool,
			Logger: s.logger,
		})
	}

	// ─── Chapter 01: Infrastructure ──────────────────────────────────────────
	r.Get("/health", handlers.Health())
	// Ch04: /ready checks all wired dependencies via the Pinger interface.
	r.Get("/ready", handlers.Ready(s.readyChecks()))

	// ─── API v1 ───────────────────────────────────────────────────────────────
	//
	// Routes are split into three authentication tiers. When s.auth is wired
	// (always, in the production binary) each group is gated; when it is nil
	// (handler-level tests) the groups mount open, preserving prior behaviour.
	r.Route("/api/v1", func(r chi.Router) {

		// Chapter 02: Prometheus metrics scrape endpoint. Public infra
		// endpoint — keep it behind network isolation in production. Labels
		// carry only route patterns + status, never tenant data.
		r.Get("/metrics", promhttp.HandlerFor(s.promReg, promhttp.HandlerOpts{}).ServeHTTP)

		// ─── Admin-scoped: operational / cross-tenant routes ─────────────
		// Require an admin key. Tenant keys are rejected (403).
		r.Group(func(r chi.Router) {
			if s.auth != nil {
				r.Use(s.auth.Authenticate, s.auth.RequireAdmin)
			}
			if s.rateLimiter != nil {
				r.Use(nexusmw.RateLimit(s.rateLimiter))
			}
			if idempotency != nil {
				r.Use(idempotency)
			}
			s.mountAdminRoutes(r)
		})

		// ─── Tenant-scoped: tenant derived from the API key ──────────────
		// Any valid tenant key. The handler reads the tenant from context —
		// never the request body — so a caller can only touch its own data.
		r.Group(func(r chi.Router) {
			if s.auth != nil {
				r.Use(s.auth.Authenticate)
			}
			if s.rateLimiter != nil {
				r.Use(nexusmw.RateLimit(s.rateLimiter))
			}
			if idempotency != nil {
				r.Use(idempotency)
			}
			s.mountTenantRoutes(r)
		})

		// ─── Tenant-scoped with {tenantID} in the path ───────────────────
		// EnforceTenantParam rejects any key whose tenant differs from the
		// path segment, so the URL can never address another tenant.
		r.Group(func(r chi.Router) {
			if s.auth != nil {
				r.Use(s.auth.Authenticate, s.auth.EnforceTenantParam)
			}
			if s.rateLimiter != nil {
				r.Use(nexusmw.RateLimit(s.rateLimiter))
			}
			if idempotency != nil {
				r.Use(idempotency)
			}
			s.mountTenantParamRoutes(r)
		})
	})

	return r
}

// mountAdminRoutes registers operational / cross-tenant endpoints. In the
// production binary these sit behind Authenticate + RequireAdmin.
func (s *Server) mountAdminRoutes(r chi.Router) {
	// Chapter 01: tenant management (list all / create) — operational, not
	// tenant-scoped, so it lives behind the admin tier.
	r.Get("/tenants", handlers.ListTenants(s.tenants))
	r.Post("/tenants", handlers.CreateTenant(s.tenants))

	// Production-hardening: API-key minting + revocation. This is how an
	// operator turns the bootstrap admin key into per-tenant keys.
	if s.apiKeys != nil {
		r.Post("/admin/api-keys", handlers.CreateAPIKey(s.apiKeys))
		r.Delete("/admin/api-keys/{keyID}", handlers.RevokeAPIKey(s.apiKeys))
	}

	// Chapter 06: replication lag monitor + routing-decision inspector.
	if s.replicaPool != nil {
		r.Get("/replication-lag", handlers.ReplicationLag(s.replicaPool))
		r.Get("/replication-status", handlers.ReplicationStatus(s.replicaPool))
	}

	// Chapter 07: shard lookup + cross-shard fan-out (the latter only when
	// per-shard pools exist).
	if s.shardRouter != nil {
		r.Get("/shard", handlers.ShardInfo(s.shardRouter))
	}
	if s.shardAdmin != nil {
		r.Get("/shard/distribution", handlers.ShardDistribution(s.shardAdmin))
		r.Get("/shard/load", handlers.ShardLoad(s.shardAdmin))
	}

	// Chapter 08: Billing via Temporal durable workflow.
	if s.billing != nil {
		r.Post("/billing/charge", handlers.Charge(s.billing))
	}

	// Chapter 09: circuit breaker status + chaos / fault-injection knobs.
	if s.breakers != nil {
		r.Get("/circuit-breakers", handlers.CircuitBreakerStatus(s.breakers))
	}
	if s.chaos != nil {
		r.Get("/chaos", handlers.ChaosState(s.chaos))
		r.Post("/chaos", handlers.ChaosSet(s.chaos))
		r.Post("/chaos/reset", handlers.ChaosReset(s.chaos))
	}

	// Chapter 09/10: fenced write demo.
	if s.fencedResource != nil {
		r.Get("/protected", handlers.FencedState(s.fencedResource))
		r.Post("/protected", handlers.FencedApply(s.fencedResource))
	}

	// Chapter 10: leader election status.
	if s.leader != nil {
		r.Get("/leader", handlers.LeaderStatus(s.leader))
	}

	// Chapter 13: projection lag. Sentinel takes precedence over runner:
	// in sharded mode the central runner reads an empty events_store, so
	// reporting its lag would mislead — the 503 handler is the honest answer.
	switch {
	case s.projectionsUnavailableReason != "":
		r.Get("/projections", handlers.ProjectionsUnavailable(s.projectionsUnavailableReason))
	case s.projectionRunner != nil:
		r.Get("/projections", handlers.ProjectionLag(s.projectionRunner))
	}
}

// mountTenantRoutes registers tenant-scoped endpoints whose tenant comes
// from the API key (no {tenantID} in the path). Behind Authenticate.
func (s *Server) mountTenantRoutes(r chi.Router) {
	// Chapter 03+: Events API — ingest, list, and search analytics events.
	// Ch05: IngestEvent also publishes to Kafka when a producer is wired.
	if s.events != nil {
		r.Post("/events", handlers.IngestEvent(s.events, s.producer))
		// Appendix A (JS SDK): batched ingest for browser/server SDKs that
		// buffer events client-side and flush every N events / T seconds.
		r.Post("/events/batch", handlers.IngestEventBatch(s.events, s.producer))
		r.Get("/events", handlers.ListEvents(s.events))
		r.Get("/events/search", handlers.SearchEvents(s.events))
	}
}

// mountTenantParamRoutes registers tenant-scoped endpoints addressed by a
// {tenantID} path segment. Behind Authenticate + EnforceTenantParam, so the
// path tenant is guaranteed to equal the caller's own tenant.
func (s *Server) mountTenantParamRoutes(r chi.Router) {
	// Chapter 01: a tenant reading its own record.
	r.Get("/tenants/{tenantID}", handlers.GetTenant(s.tenants))

	// Chapter 04: OLAP daily-stats endpoint backed by ClickHouse.
	if s.clickhouseEvents != nil {
		r.Get("/tenants/{tenantID}/daily-stats", handlers.DailyStats(s.clickhouseEvents))
	}

	// Chapter 12: live stats via SSE. Ch14: gated on "analytics" consent
	// when the checker is wired.
	if s.cache != nil {
		liveStats := handlers.LiveStats(s.cache.Client(), handlers.LiveStatsConfig{})
		if s.consentChecker != nil {
			gated := nexusmw.ConsentGate(s.consentChecker, "analytics", s.logger)(liveStats)
			r.Get("/tenants/{tenantID}/live-stats", gated.ServeHTTP)
		} else {
			r.Get("/tenants/{tenantID}/live-stats", liveStats)
		}
	}

	// Chapter 14: GDPR compliance endpoints.
	if s.gdprExporter != nil {
		r.Route("/gdpr/{tenantID}", func(r chi.Router) {
			r.Get("/export", handlers.ExportData(s.gdprExporter))
			r.Post("/erasure", handlers.ErasureRequest(s.gdprExporter))
			r.Post("/anonymise", handlers.Anonymise(s.gdprExporter))
			r.Post("/consent", handlers.ManageConsent(s.gdprConsent))
			r.Get("/consent", handlers.ListConsent(s.gdprConsent))
		})
	}
}

// readyChecks returns the set of Pingers to check in /ready.
// Built at Router() time so nil deps are simply excluded.
func (s *Server) readyChecks() map[string]handlers.Pinger {
	checks := map[string]handlers.Pinger{}
	if p, ok := s.tenants.(handlers.Pinger); ok {
		checks["postgres"] = p
	}
	if s.cache != nil {
		checks["redis"] = s.cache
	}
	if s.clickhouse != nil {
		checks["clickhouse"] = s.clickhouse
	}
	return checks
}

// exposeRequestID copies the request ID from the context to the response
// header so that writeError can include it in structured error responses.
func exposeRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if reqID := middleware.GetReqID(r.Context()); reqID != "" {
			w.Header().Set("X-Request-Id", reqID)
		}
		next.ServeHTTP(w, r)
	})
}

// metricsMiddleware records HTTP request count and latency in Prometheus.
// Ch02: introduced here. Every subsequent chapter inherits this automatically.
//
// Ch14 (review fix): the metric label is the chi *route pattern*
// (e.g. "/api/v1/tenants/{tenantID}/live-stats"), NOT r.URL.Path.
// Two reasons:
//
//   1. PII safety. r.URL.Path can carry whatever the caller put in
//      the URL — emails, phone numbers, IPs in path or query — and
//      any of those would land in Prometheus labels and persist
//      for the metric retention window (typically months). The
//      route pattern only contains what the developer wrote.
//
//   2. Cardinality. r.URL.Path makes one time series per distinct
//      URL — UUIDs in tenantID parameters explode the cardinality
//      and crater Prometheus performance. Route pattern caps it at
//      one series per route.
//
// The access log STILL prefers the masked path (from PIIDetect)
// over r.URL.Path, since logs need the actual URL for debugging
// but want the PII removed.
func (s *Server) metricsMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)

			duration := time.Since(start)
			status := fmt.Sprintf("%d", ww.Status())

			// Prefer chi's matched route pattern. Falls back to a
			// stable "unknown" sentinel for unmatched routes (404s
			// from chi.NotFound) so we don't leak the raw path
			// even there.
			metricPath := "unknown"
			if rctx := chi.RouteContext(r.Context()); rctx != nil {
				if pat := rctx.RoutePattern(); pat != "" {
					metricPath = pat
				}
			}

			// Don't record the Prometheus scrape endpoint itself.
			// Every scrape (15s default) would otherwise produce a
			// fast 200 that skews latency percentiles low and
			// inflates the denominators of the SLO error-rate
			// rules. The SLO recording rules also filter this
			// label as a defense in depth.
			if metricPath != "/api/v1/metrics" {
				s.metrics.HTTPRequests.WithLabelValues(r.Method, metricPath, status).Inc()
				s.metrics.HTTPDuration.WithLabelValues(r.Method, metricPath).Observe(duration.Seconds())
			}

			loggedPath := r.URL.Path
			if masked := nexusmw.MaskedPathFromContext(r.Context()); masked != "" {
				loggedPath = masked
			}

			s.logger.Info("request",
				zap.String("method", r.Method),
				zap.String("path", loggedPath),
				zap.String("route", metricPath),
				zap.String("status", status),
				zap.Duration("duration", duration),
				zap.String("request_id", middleware.GetReqID(r.Context())),
			)
		})
	}
}
