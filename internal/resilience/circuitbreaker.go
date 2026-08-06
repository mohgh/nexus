// Package resilience provides fault-tolerance primitives for Nexus.
//
// Ch09 teaching points:
//  1. Circuit breakers prevent cascading failures — when a dependency is
//     consistently failing, stop calling it and fail fast instead.
//  2. Three states: Closed (normal) → Open (failing fast) → Half-Open (probing).
//  3. Timeouts prevent goroutine starvation when a dependency hangs.
//  4. Fallback strategies (cached data, empty results) keep the app useful
//     when a non-critical dependency is down.
//
// DDIA reference: Chapter 8 — "Everything fails all the time."
// The circuit breaker is the operationalisation of that principle.
package resilience

import (
	"context"
	"fmt"
	"time"

	"github.com/mohgh/nexus/internal/metrics"
	"github.com/sony/gobreaker/v2"
	"go.uber.org/zap"
)

// transitionObserver is the optional Prometheus hook NewBreaker
// uses on every state change. Kept package-level so a single
// InstallMetrics call (from main.go) instruments every subsequent
// breaker without threading the registry through every constructor.
// Nil = no instrumentation.
var transitionObserver *metrics.Registry

// InstallMetrics wires the Prometheus registry that breakers
// record state transitions to. Pass nil to disable. Calling again
// replaces the previous registry.
//
// Ordering matters: call this BEFORE any NewBreaker call you want
// instrumented. The transition handler captures the registry at
// breaker-construction time.
func InstallMetrics(m *metrics.Registry) {
	transitionObserver = m
}

// Breaker wraps a gobreaker circuit breaker with structured logging.
// Use one Breaker per external dependency (postgres, redis, clickhouse, kafka).
type Breaker[T any] struct {
	cb     *gobreaker.CircuitBreaker[T]
	logger *zap.Logger
	name   string
}

// Settings configures a circuit breaker.
type Settings struct {
	// MaxRequests in half-open state before deciding if the dependency recovered.
	MaxRequests uint32
	// Interval resets failure counts in closed state (0 = never reset).
	Interval time.Duration
	// Timeout is how long the breaker stays open before switching to half-open.
	Timeout time.Duration
	// Threshold is the minimum number of requests before tripping.
	// The breaker trips when FailureRatio of requests in the last Interval fail.
	ConsecutiveFailures uint32
}

// DefaultSettings returns conservative defaults suitable for a web service.
var DefaultSettings = Settings{
	MaxRequests:         3,
	Interval:            10 * time.Second,
	Timeout:             30 * time.Second,
	ConsecutiveFailures: 5,
}

// NewBreaker creates a circuit breaker for the named dependency.
//
// State transitions are logged AND, if InstallMetrics was called
// with a non-nil registry, counted on CircuitBreakerTransitions.
// The metric reports labels (name, from, to) so an alert can fire
// specifically on closed→open transitions rather than every state
// change (most of which are recovery).
func NewBreaker[T any](name string, s Settings, logger *zap.Logger) *Breaker[T] {
	// Snapshot the observer reference at construction time. If
	// InstallMetrics is called later it won't retroactively
	// instrument earlier breakers — callers should wire metrics
	// first.
	obs := transitionObserver
	cb := gobreaker.NewCircuitBreaker[T](gobreaker.Settings{
		Name:        name,
		MaxRequests: s.MaxRequests,
		Interval:    s.Interval,
		Timeout:     s.Timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= s.ConsecutiveFailures
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			logger.Warn("circuit breaker state changed",
				zap.String("name", name),
				zap.String("from", from.String()),
				zap.String("to", to.String()),
			)
			if obs != nil {
				obs.CircuitBreakerTransitions.
					WithLabelValues(name, from.String(), to.String()).
					Inc()
			}
		},
	})
	return &Breaker[T]{cb: cb, logger: logger, name: name}
}

// Execute runs fn inside the circuit breaker.
// Returns gobreaker.ErrOpenState if the circuit is open (failing fast).
func (b *Breaker[T]) Execute(fn func() (T, error)) (T, error) {
	return b.cb.Execute(fn)
}

// State returns the current state: "closed", "half-open", or "open".
func (b *Breaker[T]) State() string {
	return b.cb.State().String()
}

// Counts returns the circuit breaker's request counters.
func (b *Breaker[T]) Counts() gobreaker.Counts {
	return b.cb.Counts()
}

// Registry holds all circuit breakers for Nexus dependencies.
// Exposed via the chaos endpoint (Ch09) for observability.
type Registry struct {
	breakers map[string]StateReporter
}

// StateReporter can report a circuit breaker's current state.
type StateReporter interface {
	State() string
	Counts() gobreaker.Counts
}

// NewRegistry creates an empty circuit breaker registry.
func NewRegistry() *Registry {
	return &Registry{breakers: make(map[string]StateReporter)}
}

// Register adds a breaker to the registry under the given name.
func (r *Registry) Register(name string, b StateReporter) {
	r.breakers[name] = b
}

// States returns the current state of all registered circuit breakers.
// Returns map[string]any so the Registry satisfies handlers.BreakerInspector
// without creating an import cycle.
func (r *Registry) States() map[string]any {
	out := make(map[string]any, len(r.breakers))
	for name, b := range r.breakers {
		counts := b.Counts()
		out[name] = map[string]any{
			"state":    b.State(),
			"requests": counts.Requests,
			"failures": counts.TotalFailures,
		}
	}
	return out
}

// Call is a convenience wrapper that adds a context deadline before calling fn.
// Use this instead of Execute when you want both a timeout AND circuit breaking.
func Call[T any](ctx context.Context, b *Breaker[T], timeout time.Duration, fn func(ctx context.Context) (T, error)) (T, error) {
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return b.Execute(func() (T, error) {
		result, err := fn(tctx)
		if err != nil {
			return result, fmt.Errorf("%s: %w", b.name, err)
		}
		return result, nil
	})
}
