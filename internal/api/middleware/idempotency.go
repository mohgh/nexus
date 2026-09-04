package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mohgh/nexus/internal/auth"
	"go.uber.org/zap"
)

// Default TTL for cached responses. Stripe's idempotency-key
// contract is 24h; we follow the same convention. Older entries
// are deleted by a cleanup goroutine (see RunIdempotencyCleanup).
const defaultIdempotencyTTL = 24 * time.Hour

// maxRequestBodyForFingerprint is the largest request body the
// middleware will buffer to compute a fingerprint. Requests larger
// than this skip the cache entirely (run uncached) — better than
// risking OOM on a giant payload to satisfy a header.
const maxRequestBodyForFingerprint = 1 << 20 // 1 MiB

// maxHandlerRuntime mirrors the router's request-level timeout
// (chi middleware.Timeout(10s), see internal/api/router.go). It is the
// ceiling on how long a request can legitimately hold an in_flight
// reservation, and so the unit that both the advertised Retry-After
// and the stale-reservation sweep are derived from. If the router's
// timeout changes, change this with it.
const maxHandlerRuntime = 10 * time.Second

// retryAfterInFlight is what a 409 IDEMPOTENCY_REQUEST_IN_PROGRESS
// advertises. It must be a number a client can actually obey.
//
// The dominant reason for seeing an in_flight row is "the original
// request is genuinely still running", and that resolves within
// maxHandlerRuntime — either the handler finishes, or the router's
// timeout fires and the cleanup below releases the reservation. So the
// honest answer is "a little more than the request timeout"; the small
// margin keeps a client that retries exactly on the deadline from
// racing the original's own cleanup.
//
// It used to say 5 seconds, which was not achievable by any mechanism
// in the system: a genuinely stuck row was cleared only by the sweeper
// (inFlightStaleTTL, below), so an obedient client burned dozens of
// guaranteed-409 retries before the row could possibly go away.
const retryAfterInFlight = maxHandlerRuntime + 2*time.Second

// cleanupTimeout bounds a reservation-cleanup DB call. Cleanup runs
// after the response is already written (or abandoned), so it must not
// pin a pool connection or a goroutine for long — but it must be long
// enough to survive an ordinary slow round trip.
const cleanupTimeout = 3 * time.Second

// cleanupContext returns the context used to release or complete a
// reservation. It is deliberately DETACHED from the request context.
//
// The reservation is a mutual-exclusion primitive, and a mutual-
// exclusion primitive must never share a lifetime with the thing it
// protects. r.Context() is cancelled exactly when the request times out
// (chi middleware.Timeout) or the client disconnects — which is to say,
// exactly in the situations that cause the client to retry. Cleaning up
// on it meant the DB call failed instantly and the in_flight row
// survived, so every retry got 409 until the sweeper ran, and the
// reservation was released only when nobody needed it released.
//
// context.WithoutCancel rather than context.Background: it drops
// cancellation and deadlines while KEEPING the request-scoped values
// (request ID, auth principal, trace span), so the warn logs on the
// failure paths below are still attributable to the request that
// leaked. The bounded timeout on top keeps a wedged database from
// holding the goroutine open indefinitely.
func cleanupContext(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(r.Context()), cleanupTimeout)
}

// IdempotencyConfig wires the middleware to a Postgres pool.
type IdempotencyConfig struct {
	Pool   *pgxpool.Pool
	TTL    time.Duration // 0 -> defaultIdempotencyTTL
	Logger *zap.Logger
}

// Idempotency returns a chi-compatible middleware. Behaviour:
//
//   - Non-mutating (GET/HEAD/OPTIONS): pass through.
//   - No Idempotency-Key header: pass through (caller opts in).
//   - Header set: reserve-then-execute. The middleware INSERTs an
//     in_flight reservation row BEFORE running the handler. The
//     reservation IS the dedup boundary: concurrent retries can't
//     both insert (PRIMARY KEY conflict), so concurrent retries see
//     each other.
//   - Reservation conflict, existing row state='completed' AND
//     fingerprint matches: return cached body with X-Idempotent-Replay.
//   - Reservation conflict, completed AND fingerprint differs:
//     return 409 IDEMPOTENCY_KEY_REUSED.
//   - Reservation conflict, existing row state='in_flight':
//     return 409 IDEMPOTENCY_REQUEST_IN_PROGRESS with Retry-After
//     so the caller knows to back off. Stale in_flight rows
//     (handler crashed mid-request) are cleaned up by the
//     companion goroutine — see RunIdempotencyCleanup.
//   - Original execution: on 2xx UPDATE state='completed' with the
//     captured response; on non-2xx DELETE the reservation so
//     retries can proceed cleanly; on panic the deferred cleanup
//     deletes the reservation and the chi Recoverer (outer) writes
//     the 500. All three run on a detached, short-deadline context
//     (see cleanupContext) so the reservation is released even when
//     the request itself timed out or the client hung up.
//
// Storing only 2xx responses matches Stripe's behaviour: a transient
// 500 is retryable. Storing the entire response (status +
// content-type + body) means a replay returns the exact same bytes
// — no header drift, no body re-render.
//
// The reserve-then-execute shape is what makes concurrent retries
// truly safe. The earlier lookup-then-execute version had a documented
// race window where both retries could run the handler before either
// committed the cache row, and side-effecting handlers (e.g. POST
// /api/v1/events) would write twice.
func Idempotency(cfg IdempotencyConfig) func(http.Handler) http.Handler {
	if cfg.Pool == nil {
		panic("middleware.Idempotency: Pool is required")
	}
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = defaultIdempotencyTTL
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !isMutating(r.Method) {
				next.ServeHTTP(w, r)
				return
			}
			key := r.Header.Get("Idempotency-Key")
			if key == "" {
				// Appendix A (JS SDK): browsers cannot set custom
				// headers on navigator.sendBeacon, so the SDK puts
				// the key in the query string on its unload-path
				// flushes. Header still wins when both are present
				// (defensive against a malicious proxy adding a
				// query param to a request that already had a
				// header).
				key = r.URL.Query().Get("idempotency_key")
			}
			if key == "" {
				next.ServeHTTP(w, r)
				return
			}

			// Namespace the key by the authenticated principal so one
			// tenant's Idempotency-Key can never collide with — or replay
			// the cached response of — another tenant's. This middleware
			// now runs AFTER Authenticate (see router.go), so the principal
			// is already in context. An unauthenticated / test context maps
			// to a single shared namespace, preserving prior behaviour.
			//
			// This matters specifically because the events endpoints derive
			// their tenant from the API key, not the request body — so the
			// (method, path, body) fingerprint alone is identical across
			// tenants posting the same payload. Without this namespace, two
			// tenants reusing a key would cross-replay.
			key = idempotencyNamespace(r.Context()) + "|" + key

			// Buffer the body and rewind r.Body so the handler can
			// still read it. Bodies larger than the cap fall through
			// uncached — see comment on maxRequestBodyForFingerprint.
			body, oversize, err := readBodyForFingerprint(r)
			if err != nil {
				logger.Warn("idempotency: read body failed (continuing without cache)",
					zap.Error(err),
				)
				next.ServeHTTP(w, r)
				return
			}
			if oversize {
				logger.Warn("idempotency: body too large for fingerprint cache; running uncached",
					zap.Int64("max_bytes", maxRequestBodyForFingerprint),
				)
				next.ServeHTTP(w, r)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))

			// Fingerprint scopes the cache to the (method, path, body)
			// of THIS request — without it, reusing the same key on a
			// different route or with a different payload would replay
			// the original cached 2xx for the new request. Cross-tenant
			// scoping is handled by the key namespace above (the path is
			// not always tenant-distinct, e.g. /api/v1/events).
			fingerprint := requestFingerprint(r.Method, r.URL.Path, body)

			// Try to claim the key with an in_flight reservation. The
			// INSERT either succeeds (we won the race; run the handler)
			// or conflicts (someone else has it; check their state).
			reserved, err := tryReserveInFlight(r.Context(), cfg.Pool, key, fingerprint)
			if err != nil {
				logger.Warn("idempotency: reservation failed (continuing without cache)",
					zap.Error(err),
				)
				// Soft-fail: better to run the handler than 500 on a
				// transient DB error.
				next.ServeHTTP(w, r)
				return
			}

			if !reserved {
				// Conflict: the row exists. Read it to decide what to do.
				existing, err := lookupReservation(r.Context(), cfg.Pool, key, ttl)
				if err != nil {
					logger.Warn("idempotency: lookup after conflict failed (continuing without cache)",
						zap.Error(err),
					)
					next.ServeHTTP(w, r)
					return
				}
				if existing == nil {
					// Race resolved between our INSERT and SELECT —
					// the other request finished and (because it was
					// non-2xx) deleted the row. Treat as miss; run
					// uncached to avoid an infinite reservation loop.
					next.ServeHTTP(w, r)
					return
				}

				switch existing.State {
				case stateCompleted:
					if !bytes.Equal(existing.Fingerprint, fingerprint) {
						http.Error(w,
							`{"error":"Idempotency-Key reused with different request fingerprint","code":"IDEMPOTENCY_KEY_REUSED"}`,
							http.StatusConflict,
						)
						return
					}
					w.Header().Set("Content-Type", existing.ContentType)
					w.Header().Set("X-Idempotent-Replay", "true")
					w.WriteHeader(existing.Status)
					_, _ = w.Write(existing.Body)
					return

				case stateInFlight:
					// Original is still running. Tell the client to
					// back off — replaying nothing yet is correct;
					// double-running the handler is what we're
					// preventing here.
					w.Header().Set("Retry-After",
						strconv.Itoa(int(retryAfterInFlight.Seconds())))
					http.Error(w,
						`{"error":"request with this Idempotency-Key is in progress","code":"IDEMPOTENCY_REQUEST_IN_PROGRESS"}`,
						http.StatusConflict,
					)
					return
				}
			}

			// We hold the reservation. Run the handler, capture its
			// response. The defer ensures the reservation is cleaned
			// up if the handler panics — without it, a stuck row
			// would block retries until the cleanup goroutine swept.
			//
			// EVERY exit path below cleans up on cleanupContext(r),
			// never on r.Context(). See cleanupContext for why.
			rec := newRecordingWriter(w)
			normalExit := false
			defer func() {
				if normalExit {
					return
				}
				// Panic path: drop the reservation so the chi
				// Recoverer (outer) can write 500 and a future retry
				// can proceed.
				ctx, cancel := cleanupContext(r)
				defer cancel()
				if delErr := deleteReservation(ctx, cfg.Pool, key); delErr != nil {
					logger.Warn("idempotency: cleanup after panic failed",
						zap.String("key", key),
						zap.Error(delErr),
					)
				}
			}()
			next.ServeHTTP(rec, r)

			cleanupCtx, cancelCleanup := cleanupContext(r)
			defer cancelCleanup()

			if rec.status >= 200 && rec.status < 300 {
				// Cache the 2xx even if the request context is already
				// dead. A 2xx means the handler's side effects
				// committed; the reservation exists to stop those from
				// happening twice, and the cached response is the only
				// record that they happened at all. Dropping it because
				// the caller went away would let the retry re-run the
				// handler and duplicate the write — precisely the
				// failure this middleware exists to prevent. The client
				// may never have received these bytes (chi's Timeout
				// turns the response into a 504, a disconnected client
				// receives nothing), but that is an argument FOR
				// caching: the retry then gets the true outcome of the
				// work instead of repeating it.
				ct := rec.Header().Get("Content-Type")
				if ct == "" {
					ct = "application/json"
				}
				if err := completeReservation(cleanupCtx, cfg.Pool,
					key, rec.status, rec.body.Bytes(), ct,
				); err != nil {
					logger.Warn("idempotency: complete failed",
						zap.String("key", key),
						zap.Error(err),
					)
				}
			} else {
				// Non-2xx: drop the reservation so a retry isn't
				// stuck on a transient failure (the in_flight row
				// would otherwise return 409 until cleanup). This is
				// the timed-out-request path too: chi's Timeout
				// cancels the context, the handler bails with a 5xx,
				// and the reservation must be released so the retry
				// the client is about to send can actually run.
				if err := deleteReservation(cleanupCtx, cfg.Pool, key); err != nil {
					logger.Warn("idempotency: delete on non-2xx failed",
						zap.String("key", key),
						zap.Error(err),
					)
				}
			}
			normalExit = true
		})
	}
}

// reservation states stored in processed_idempotency_keys.state.
const (
	stateInFlight = "in_flight"
	stateCompleted = "completed"
)

// readBodyForFingerprint reads up to maxRequestBodyForFingerprint+1
// bytes from r.Body and returns (body, oversize, err). When
// oversize is true the body exceeds the cap and the caller should
// fall through without caching. When err is non-nil the body could
// not be read at all.
func readBodyForFingerprint(r *http.Request) ([]byte, bool, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, false, nil
	}
	limited := io.LimitReader(r.Body, maxRequestBodyForFingerprint+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, false, err
	}
	if int64(len(body)) > maxRequestBodyForFingerprint {
		return nil, true, nil
	}
	return body, false, nil
}

// requestFingerprint returns a 32-byte sha256 over the method,
// path, and body. The same logical request always produces the
// same fingerprint; any change to method / path / body changes it.
func requestFingerprint(method, path string, body []byte) []byte {
	h := sha256.New()
	h.Write([]byte(method))
	h.Write([]byte("\n"))
	h.Write([]byte(path))
	h.Write([]byte("\n"))
	h.Write(body)
	return h.Sum(nil)
}

// idempotencyNamespace returns the per-principal prefix that scopes a
// stored idempotency key. Tenant principals get "t:<tenantID>"; admin
// (tenant-less) principals get "k:<keyID>" so each admin key has its own
// namespace; an unauthenticated/test context gets "" (a single shared
// namespace, matching pre-auth behaviour).
func idempotencyNamespace(ctx context.Context) string {
	p, ok := auth.PrincipalFromContext(ctx)
	if !ok {
		return ""
	}
	if p.TenantID != "" {
		return "t:" + p.TenantID
	}
	return "k:" + p.KeyID
}

func isMutating(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	}
	return false
}

// ─── Response capture ──────────────────────────────────────────────────────

type recordingWriter struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func newRecordingWriter(w http.ResponseWriter) *recordingWriter {
	return &recordingWriter{ResponseWriter: w, status: 0}
}

func (r *recordingWriter) WriteHeader(s int) {
	if r.status == 0 {
		r.status = s
	}
	r.ResponseWriter.WriteHeader(s)
}

func (r *recordingWriter) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	r.body.Write(b)
	return r.ResponseWriter.Write(b)
}

// Flush delegates to the underlying writer. The idempotency
// middleware only wraps mutating requests today, so streaming
// endpoints typically don't reach this wrapper — but a future
// caller that adds an Idempotency-Key to a long-lived endpoint
// would otherwise lose Flusher silently. Keeps the wrapper
// transparent to streaming concerns.
func (r *recordingWriter) Flush() {
	if fl, ok := r.ResponseWriter.(http.Flusher); ok {
		fl.Flush()
	}
}

// ─── Storage ───────────────────────────────────────────────────────────────

// reservation is what lookupReservation returns. Status / Body /
// ContentType are populated only when State == stateCompleted; for
// in_flight rows the response columns are NULL in Postgres and
// represented as zero values here.
type reservation struct {
	State       string
	Fingerprint []byte
	Status      int
	Body        []byte
	ContentType string
}

// tryReserveInFlight attempts to claim the key with an in_flight
// row. Returns (true, nil) iff the row was inserted (we own the
// reservation). On conflict, returns (false, nil) — the caller
// should look up the existing row and decide what to do.
func tryReserveInFlight(ctx context.Context, pool *pgxpool.Pool, key string, fingerprint []byte) (bool, error) {
	tag, err := pool.Exec(ctx,
		`INSERT INTO processed_idempotency_keys
		 (idempotency_key, request_fingerprint, state, created_at)
		 VALUES ($1, $2, 'in_flight', NOW())
		 ON CONFLICT (idempotency_key) DO NOTHING`,
		key, fingerprint,
	)
	if err != nil {
		return false, fmt.Errorf("reserve idempotency key: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// lookupReservation reads an existing row by key. Returns nil with
// no error if the row is gone (race resolved between INSERT and
// SELECT, or the row aged out). Stale completed rows past TTL are
// also returned as nil — the cleanup goroutine will delete them.
func lookupReservation(ctx context.Context, pool *pgxpool.Pool, key string, ttl time.Duration) (*reservation, error) {
	var (
		state       string
		fingerprint []byte
		status      *int
		body        []byte
		ct          *string
		age         time.Duration
	)
	err := pool.QueryRow(ctx,
		`SELECT state, request_fingerprint,
		        response_status, response_body, response_content_type,
		        NOW() - created_at
		 FROM processed_idempotency_keys
		 WHERE idempotency_key = $1`,
		key,
	).Scan(&state, &fingerprint, &status, &body, &ct, &age)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup idempotency reservation: %w", err)
	}
	if state == stateCompleted && age > ttl {
		// Expired completion. Cleanup goroutine will delete; treat
		// as miss for now.
		return nil, nil
	}

	out := &reservation{State: state, Fingerprint: fingerprint}
	if status != nil {
		out.Status = *status
	}
	out.Body = body
	if ct != nil {
		out.ContentType = *ct
	}
	return out, nil
}

// completeReservation transitions an in_flight row to completed
// with the captured response. The WHERE clause keys on state so a
// re-run can't accidentally overwrite an already-completed row
// (defensive against the unlikely double-defer case).
func completeReservation(ctx context.Context, pool *pgxpool.Pool, key string, status int, body []byte, contentType string) error {
	_, err := pool.Exec(ctx,
		`UPDATE processed_idempotency_keys
		 SET response_status = $1,
		     response_body = $2,
		     response_content_type = $3,
		     state = 'completed',
		     created_at = NOW()
		 WHERE idempotency_key = $4 AND state = 'in_flight'`,
		status, body, contentType, key,
	)
	if err != nil {
		return fmt.Errorf("complete idempotency reservation: %w", err)
	}
	return nil
}

// deleteReservation removes an in_flight row so retries can proceed
// after a non-2xx response or a panic. Completed rows are protected
// by the state filter — we never delete a row that already has a
// cached response.
func deleteReservation(ctx context.Context, pool *pgxpool.Pool, key string) error {
	_, err := pool.Exec(ctx,
		`DELETE FROM processed_idempotency_keys
		 WHERE idempotency_key = $1 AND state = 'in_flight'`,
		key,
	)
	if err != nil {
		return fmt.Errorf("delete idempotency reservation: %w", err)
	}
	return nil
}

// inFlightStaleTTL is how long an in_flight reservation is allowed
// to live before the cleanup considers it abandoned (e.g. the
// original process was killed between the INSERT and the deferred
// cleanup).
//
// The bound that matters is maxHandlerRuntime: the router caps every
// request at 10s, so no live request can hold a reservation longer
// than that, and the cleanup paths above now release it even when the
// request times out or the client disconnects. 6x that ceiling leaves
// a wide margin for clock skew and a slow cleanup round trip while
// keeping the abandoned-row window near the Retry-After we advertise.
//
// The margin is the safety-critical direction: sweeping a reservation
// that is still live would let a concurrent retry run the handler a
// second time — the exact double-execution this design prevents. Err
// long, never short.
//
// It used to be 5 minutes, chosen when cleanup routinely failed and
// the sweeper was the primary release mechanism rather than the
// backstop it is now.
const inFlightStaleTTL = 6 * maxHandlerRuntime

// inFlightSweepInterval is the sweep cadence. Worst-case time for an
// abandoned reservation to clear is inFlightStaleTTL +
// inFlightSweepInterval (75s), so a client obeying Retry-After
// (12s) reaches it in a handful of retries rather than the ~60 the
// old 5-second header implied against a 6-minute worst case.
const inFlightSweepInterval = 15 * time.Second

// completedSweepInterval is the cadence for expiring cached responses.
// Unchanged from the original 1-minute tick: those rows live for the
// full TTL (24h by default), so sweeping them more often only repeats
// a scan that no index supports.
const completedSweepInterval = time.Minute

// RunIdempotencyCleanup loops until ctx is cancelled, deleting
// expired cache rows. Two separate TTLs, on two separate cadences:
//
//   - state='completed' rows: TTL (default 24h), swept every
//     completedSweepInterval — the cache window after which a retry
//     runs the handler again.
//   - state='in_flight' rows: inFlightStaleTTL, swept every
//     inFlightSweepInterval — anything older is
//     assumed to be a stuck reservation from a crashed process and is
//     deleted so retries can proceed. This is a backstop only: the
//     middleware releases its own reservation on every exit path,
//     including request timeout and client disconnect.
//
// The cleanup is a Postgres DELETE per state and holds no global
// state, so it's safe to run from every instance simultaneously.
func RunIdempotencyCleanup(ctx context.Context, pool *pgxpool.Pool, ttl time.Duration, logger *zap.Logger) {
	if ttl <= 0 {
		ttl = defaultIdempotencyTTL
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	// Two cadences, deliberately. The in_flight sweep is the backstop
	// behind the advertised Retry-After, so it must be frequent — and
	// it is cheap, riding the partial index from migration 010. The
	// completed sweep has no supporting index and a 24h TTL, so
	// running it any faster than the original 1-minute cadence would
	// buy nothing and cost a repeated scan of the whole table.
	inFlightTicker := time.NewTicker(inFlightSweepInterval)
	defer inFlightTicker.Stop()
	completedTicker := time.NewTicker(completedSweepInterval)
	defer completedTicker.Stop()

	sweepCompleted := func() {
		// Completed rows past their TTL.
		if tag, err := pool.Exec(ctx,
			`DELETE FROM processed_idempotency_keys
			 WHERE state = 'completed' AND created_at < NOW() - $1::interval`,
			fmt.Sprintf("%d seconds", int(ttl.Seconds())),
		); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Warn("idempotency cleanup: completed sweep failed", zap.Error(err))
			}
		} else if n := tag.RowsAffected(); n > 0 {
			logger.Info("idempotency cleanup: completed rows deleted",
				zap.Int64("rows", n),
			)
		}
	}

	sweepInFlight := func() {
		// Stuck in_flight rows from crashed processes. The partial
		// index on (created_at) WHERE state='in_flight' (migration
		// 010) keeps this scan tight.
		if tag, err := pool.Exec(ctx,
			`DELETE FROM processed_idempotency_keys
			 WHERE state = 'in_flight' AND created_at < NOW() - $1::interval`,
			fmt.Sprintf("%d seconds", int(inFlightStaleTTL.Seconds())),
		); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Warn("idempotency cleanup: in_flight sweep failed", zap.Error(err))
			}
		} else if n := tag.RowsAffected(); n > 0 {
			logger.Info("idempotency cleanup: stale in_flight reservations deleted",
				zap.Int64("rows", n),
			)
		}
	}

	// Run both once on startup, then on their own cadences.
	sweepCompleted()
	sweepInFlight()
	for {
		select {
		case <-ctx.Done():
			return
		case <-inFlightTicker.C:
			sweepInFlight()
		case <-completedTicker.C:
			sweepCompleted()
		}
	}
}
