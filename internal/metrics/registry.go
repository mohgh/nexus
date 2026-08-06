// Package metrics defines the Prometheus instruments used across Nexus.
//
// Ch02: HTTP request counters and latency histograms.
// Later chapters add their own instruments here (e.g. cache hit rate in Ch04,
// Kafka lag in Ch12) so everything is registered in one place.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Registry holds all Prometheus instruments for Nexus.
type Registry struct {
	// HTTPRequests counts requests by method, path, and HTTP status code.
	HTTPRequests *prometheus.CounterVec

	// HTTPDuration records request latency by method and path.
	// Use a histogram (not a summary) so percentiles can be aggregated
	// across multiple replicas — summaries cannot be merged.
	HTTPDuration *prometheus.HistogramVec

	// Ch04: cache hit/miss/error counters. Labelled by cache name
	// ("tenants", "events", ...) so a single Registry instruments
	// every cache wired in the codebase without needing a new
	// counter per cache.
	CacheHits   *prometheus.CounterVec
	CacheMisses *prometheus.CounterVec
	CacheErrors *prometheus.CounterVec

	// Ch04: OLAP query latency, labelled by engine and query name.
	// Lets the chapter's benchmark + dashboards compare Postgres vs
	// ClickHouse on the same logical aggregation.
	OLAPQueryDuration *prometheus.HistogramVec

	// Ch09: circuit-breaker state-transition counter. Labels by
	// breaker name plus from→to so an alert can fire specifically
	// on the closed→open transition (the interesting one) rather
	// than every state change. /api/v1/circuit-breakers already
	// reports the *current* state — this counter records the
	// history so you can detect rapid flapping (open ↔ half-open)
	// after the fact.
	CircuitBreakerTransitions *prometheus.CounterVec
}

// New creates and registers all instruments on the given Prometheus registerer.
// Pass prometheus.DefaultRegisterer in production; pass a fresh
// prometheus.NewRegistry() in tests to avoid cross-test pollution.
func New(reg prometheus.Registerer) *Registry {
	r := &Registry{
		HTTPRequests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "nexus_http_requests_total",
				Help: "Total HTTP requests partitioned by method, path, and status.",
			},
			[]string{"method", "path", "status"},
		),
		HTTPDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "nexus_http_request_duration_seconds",
				Help: "HTTP request latency in seconds.",
				// Buckets tuned for a web API: 1ms → 10s.
				Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			},
			[]string{"method", "path"},
		),
		CacheHits: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "nexus_cache_hits_total",
				Help: "Cache hits partitioned by cache name.",
			},
			[]string{"cache"},
		),
		CacheMisses: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "nexus_cache_misses_total",
				Help: "Cache misses partitioned by cache name.",
			},
			[]string{"cache"},
		),
		CacheErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "nexus_cache_errors_total",
				Help: "Cache lookup/store errors partitioned by cache name and operation.",
			},
			[]string{"cache", "op"},
		),
		OLAPQueryDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "nexus_olap_query_duration_seconds",
				Help: "OLAP query latency partitioned by engine and query name.",
				// Wider tail than HTTP — OLAP queries can legitimately
				// take seconds even on healthy systems.
				Buckets: []float64{.001, .01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
			},
			[]string{"engine", "query"},
		),
		CircuitBreakerTransitions: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "nexus_circuit_breaker_transitions_total",
				Help: "Circuit breaker state transitions, partitioned by breaker name and from/to state.",
			},
			[]string{"name", "from", "to"},
		),
	}

	reg.MustRegister(
		r.HTTPRequests,
		r.HTTPDuration,
		r.CacheHits,
		r.CacheMisses,
		r.CacheErrors,
		r.OLAPQueryDuration,
		r.CircuitBreakerTransitions,
	)
	return r
}
