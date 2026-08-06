package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
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
//     the 500.
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
					w.Header().Set("Retry-After", "5")
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
			rec := newRecordingWriter(w)
			normalExit := false
			defer func() {
				if normalExit {
					return
				}
				// Panic path: drop the reservation so the chi
				// Recoverer (outer) can write 500 and a future retry
				// can proceed.
				if delErr := deleteReservation(context.Background(), cfg.Pool, key); delErr != nil {
					logger.Warn("idempotency: cleanup after panic failed",
						zap.String("key", key),
						zap.Error(delErr),
					)
				}
			}()
			next.ServeHTTP(rec, r)

			if rec.status >= 200 && rec.status < 300 {
				ct := rec.Header().Get("Content-Type")
				if ct == "" {
					ct = "application/json"
				}
				if err := completeReservation(r.Context(), cfg.Pool,
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
				// would otherwise return 409 until cleanup).
				if err := deleteReservation(r.Context(), cfg.Pool, key); err != nil {
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
// original process died between INSERT and the deferred delete).
// Picked to comfortably exceed any reasonable handler runtime.
const inFlightStaleTTL = 5 * time.Minute

// RunIdempotencyCleanup loops until ctx is cancelled, deleting
// expired cache rows on a 1-minute cadence. Two separate TTLs:
//
//   - state='completed' rows: TTL (default 24h) — the cache window
//     after which a retry runs the handler again.
//   - state='in_flight' rows: 5 minutes — anything older is assumed
//     to be a stuck reservation from a crashed process and is
//     deleted so retries can proceed.
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

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	sweep := func() {
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
	// Run once on startup, then on cadence.
	sweep()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
		}
	}
}
