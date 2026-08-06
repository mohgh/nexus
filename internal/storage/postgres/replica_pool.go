package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReplicaPool routes reads to a follower and writes to the primary,
// transparently providing read-your-own-writes (RYOW) via WAL LSN tracking.
//
// Ch06 teaching points:
//
//  1. Asynchronous single-leader replication: writes go to the primary,
//     which streams the WAL to followers. Followers replay asynchronously,
//     so they may be behind by anything from microseconds to seconds.
//
//  2. RYOW via LSN: every write advances the primary's WAL Log Sequence
//     Number (LSN). A subsequent read can require "no replica reads until
//     you've seen LSN X." If the replica's replay LSN is past X, use the
//     replica; otherwise fall back to the primary. Same trick used by
//     CockroachDB follower reads and Vitess VTGate.
//
//  3. Pool-wide vs per-request RYOW: the pool keeps a global high-watermark
//     `lastWriteLSN` (every write since startup). This is conservative —
//     reads never see stale data, but reads from one tenant might block on
//     writes from another. For tighter scoping, set MinLSN per-request via
//     WithMinLSN(ctx, lsn) — the RYOW middleware does this from the
//     X-Nexus-Min-LSN request header.
type ReplicaPool struct {
	primary *Pool         // owned by the caller — we don't close it
	replica *pgxpool.Pool // owned by us; nil = primary-only mode

	// lastWriteLSN is the highest WAL LSN observed after a write since
	// startup. Stored as the numeric distance from LSN 0/0, which is
	// what pg_wal_lsn_diff returns. Monotonic.
	lastWriteLSN atomic.Uint64
}

// NewReplicaPool wraps an existing primary pool and optionally opens a
// connection pool to the replica. If replicaDSN is empty or the replica
// is unreachable on startup, the pool runs in primary-only mode (all
// reads go to the primary). The caller still owns and must close primary.
func NewReplicaPool(ctx context.Context, primary *Pool, replicaDSN string) (*ReplicaPool, error) {
	rp := &ReplicaPool{primary: primary}
	if replicaDSN == "" {
		return rp, nil
	}

	cfg, err := pgxpool.ParseConfig(replicaDSN)
	if err != nil {
		return nil, fmt.Errorf("replica_pool: parse replica DSN: %w", err)
	}
	cfg.MaxConns = 20

	replica, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("replica_pool: open replica: %w", err)
	}
	if err := replica.Ping(ctx); err != nil {
		// Degrade gracefully: replica unreachable on startup -> primary-only.
		replica.Close()
		return rp, nil
	}
	rp.replica = replica
	return rp, nil
}

// Close shuts down the replica pool. The primary is owned by the caller.
func (rp *ReplicaPool) Close() {
	if rp.replica != nil {
		rp.replica.Close()
	}
}

// HasReplica reports whether a replica is configured and reachable.
func (rp *ReplicaPool) HasReplica() bool {
	return rp.replica != nil
}

// Primary returns the primary pool — used for transactions, advisory locks,
// or any case where the routing helpers below are not enough.
func (rp *ReplicaPool) Primary() *Pool {
	return rp.primary
}

// Ping checks the primary. Satisfies the handlers.Pinger interface.
func (rp *ReplicaPool) Ping(ctx context.Context) error {
	return rp.primary.Ping(ctx)
}

// ─── Read/write routing ─────────────────────────────────────────────────────

// RecordPostWriteLSN reads the primary's current WAL LSN and records
// it on both the pool-wide watermark and the per-request recorder
// (if one is attached to ctx). Use this after a write that bypassed
// Exec — for example, a write that ran inside a caller-managed
// transaction. Best-effort: on read failure we just don't update
// the watermark.
func (rp *ReplicaPool) RecordPostWriteLSN(ctx context.Context) {
	var lsn int64
	if scanErr := rp.primary.QueryRow(ctx,
		`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint`,
	).Scan(&lsn); scanErr == nil && lsn > 0 {
		if rp.replica != nil {
			rp.recordLSN(uint64(lsn))
		}
		if rec := writeLSNRecorderFromContext(ctx); rec != nil {
			rec.Record(uint64(lsn))
		}
	}
}

// Exec runs a write on the primary and records the resulting WAL LSN in
// two places:
//
//  1. The pool-wide high-watermark (used as the default floor for reads
//     with no per-request override).
//
//  2. The per-request WriteLSNRecorder in ctx, if one is attached. The
//     RYOW middleware uses this to stamp X-Nexus-Write-LSN with the LSN
//     advanced by THIS request — not by some unrelated concurrent write
//     that happened to bump the global watermark.
//
// If the LSN read fails (a network blip, for example), the write itself
// still succeeds — we just lose the chance to record the LSN.
func (rp *ReplicaPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tag, err := rp.primary.Exec(ctx, sql, args...)
	if err != nil {
		return tag, err
	}
	// Even in primary-only mode we record the LSN on a per-request recorder
	// if one is attached — the X-Nexus-Write-LSN response stamping is useful
	// for clients planning future reads, not just for routing decisions.
	var lsn int64
	if scanErr := rp.primary.QueryRow(ctx,
		`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint`,
	).Scan(&lsn); scanErr == nil && lsn > 0 {
		if rp.replica != nil {
			rp.recordLSN(uint64(lsn))
		}
		if rec := writeLSNRecorderFromContext(ctx); rec != nil {
			rec.Record(uint64(lsn))
		}
	}
	return tag, nil
}

// Query runs a read on the replica when safe (replica caught up to the
// required LSN), otherwise falls back to the primary.
func (rp *ReplicaPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return rp.read(ctx).Query(ctx, sql, args...)
}

// QueryRow runs a single-row read on the replica when safe, otherwise on
// the primary. See Query for the routing logic.
func (rp *ReplicaPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return rp.read(ctx).QueryRow(ctx, sql, args...)
}

// read returns the pool to read from given the current routing inputs.
//
// Routing decision:
//   - No replica configured -> primary
//   - Required LSN is 0 (no writes seen yet) -> replica
//   - Replica replay LSN >= required -> replica
//   - Otherwise (replica is behind, or LSN read failed) -> primary
func (rp *ReplicaPool) read(ctx context.Context) *pgxpool.Pool {
	if rp.replica == nil {
		return rp.primary.Pool
	}

	required := requiredLSNForRead(ctx, rp.lastWriteLSN.Load())
	if required == 0 {
		return rp.replica
	}

	var replayLSN int64
	err := rp.replica.QueryRow(ctx,
		`SELECT pg_wal_lsn_diff(pg_last_wal_replay_lsn(), '0/0')::bigint`,
	).Scan(&replayLSN)
	if err != nil || replayLSN < 0 {
		return rp.primary.Pool
	}
	if uint64(replayLSN) >= required {
		return rp.replica
	}
	return rp.primary.Pool
}

// requiredLSNForRead returns the LSN a read must clear to use the
// replica: per-request override if present, else the pool-wide
// high-watermark. Pulled out of read/RoutingStatus so the precedence
// can be unit-tested without a live replica.
func requiredLSNForRead(ctx context.Context, watermark uint64) uint64 {
	if v := MinLSNFromContext(ctx); v != 0 {
		return v
	}
	return watermark
}

// ─── LSN tracking ────────────────────────────────────────────────────────────

// recordLSN advances the pool-wide high-watermark monotonically.
func (rp *ReplicaPool) recordLSN(lsn uint64) {
	for {
		old := rp.lastWriteLSN.Load()
		if lsn <= old {
			return
		}
		if rp.lastWriteLSN.CompareAndSwap(old, lsn) {
			return
		}
	}
}

// LastWriteLSN returns the highest WAL LSN observed by Exec since startup.
// Used by RYOW middleware to populate the X-Nexus-Write-LSN response header.
func (rp *ReplicaPool) LastWriteLSN() uint64 {
	return rp.lastWriteLSN.Load()
}

// ─── Routing visibility ──────────────────────────────────────────────────────

// RoutingStatus snapshots the inputs to the read-routing decision for
// the current request. Returned by GET /api/v1/replication-status.
//
// `RequiredLSN` and `WouldRouteToReplica` reflect THIS request: if the
// caller passed X-Nexus-Min-LSN, that LSN is the floor; otherwise the
// pool-wide high-watermark is. This means the status endpoint mirrors
// the actual routing decision the read path would make for the same
// request, which is the point of having an inspector at all.
type RoutingStatus struct {
	HasReplica           bool   `json:"has_replica"`
	PrimaryLSN           uint64 `json:"primary_lsn"`
	ReplicaReplayLSN     uint64 `json:"replica_replay_lsn,omitempty"`
	LagBytes             uint64 `json:"lag_bytes"`
	LastRecordedWriteLSN uint64 `json:"last_recorded_write_lsn"`
	RequiredLSN          uint64 `json:"required_lsn"`
	WouldRouteToReplica  bool   `json:"would_route_to_replica"`
}

// RoutingStatus returns a snapshot of the routing inputs for the current
// request. The decision honors MinLSNFromContext(ctx) when present so
// the endpoint matches what the read path would actually do.
func (rp *ReplicaPool) RoutingStatus(ctx context.Context) (RoutingStatus, error) {
	st := RoutingStatus{
		HasReplica:           rp.replica != nil,
		LastRecordedWriteLSN: rp.lastWriteLSN.Load(),
	}

	// Required LSN: per-request override wins, otherwise pool watermark.
	// Same precedence as ReplicaPool.read.
	st.RequiredLSN = requiredLSNForRead(ctx, st.LastRecordedWriteLSN)

	var primaryLSN int64
	if err := rp.primary.QueryRow(ctx,
		`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint`,
	).Scan(&primaryLSN); err != nil {
		return st, fmt.Errorf("routing status: primary lsn: %w", err)
	}
	if primaryLSN > 0 {
		st.PrimaryLSN = uint64(primaryLSN)
	}

	if rp.replica == nil {
		// Primary-only: there is no separate replica to consider. Surface
		// `would_route_to_replica = false` so the field is unambiguous.
		return st, nil
	}

	var replayLSN int64
	if err := rp.replica.QueryRow(ctx,
		`SELECT pg_wal_lsn_diff(pg_last_wal_replay_lsn(), '0/0')::bigint`,
	).Scan(&replayLSN); err == nil && replayLSN >= 0 {
		st.ReplicaReplayLSN = uint64(replayLSN)
		if st.PrimaryLSN > st.ReplicaReplayLSN {
			st.LagBytes = st.PrimaryLSN - st.ReplicaReplayLSN
		}
		st.WouldRouteToReplica = uint64(replayLSN) >= st.RequiredLSN
	}

	return st, nil
}

// ReplicationLag returns how far the replica is behind the primary in
// bytes, measured against the replica's *replay* position — the same
// position the read-routing decision uses.
//
// Subtle but important: pg_stat_replication.sent_lsn measures what the
// primary has sent, but a record can be sent yet not yet replayed by
// the replica (it still has to write and apply WAL). For routing
// safety we care about replay lag, not transport lag, so this query
// joins the primary's current LSN against the replica's replay LSN
// directly. With no replica configured or no replay LSN visible
// (replication slot inactive), returns 0.
func (rp *ReplicaPool) ReplicationLag(ctx context.Context) (uint64, error) {
	if rp.replica == nil {
		return 0, nil
	}

	var primaryLSN int64
	if err := rp.primary.QueryRow(ctx,
		`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint`,
	).Scan(&primaryLSN); err != nil {
		return 0, fmt.Errorf("replication lag: primary lsn: %w", err)
	}

	var replayLSN int64
	if err := rp.replica.QueryRow(ctx,
		`SELECT pg_wal_lsn_diff(pg_last_wal_replay_lsn(), '0/0')::bigint`,
	).Scan(&replayLSN); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("replication lag: replay lsn: %w", err)
	}

	if primaryLSN <= replayLSN {
		return 0, nil
	}
	return uint64(primaryLSN - replayLSN), nil
}

// ─── Per-request MinLSN context ──────────────────────────────────────────────

type minLSNKey struct{}

// WithMinLSN returns a context that requires reads to see at least LSN `lsn`.
// The RYOW middleware sets this from the X-Nexus-Min-LSN request header.
func WithMinLSN(ctx context.Context, lsn uint64) context.Context {
	return context.WithValue(ctx, minLSNKey{}, lsn)
}

// MinLSNFromContext returns the per-request minimum LSN, or 0 if none.
func MinLSNFromContext(ctx context.Context) uint64 {
	if v, ok := ctx.Value(minLSNKey{}).(uint64); ok {
		return v
	}
	return 0
}

// ─── Per-request write-LSN recorder ──────────────────────────────────────────

// WriteLSNRecorder is a per-request scratch slot that ReplicaPool.Exec
// updates whenever a write succeeds. The RYOW middleware initializes
// one for each incoming request and reads it after the handler returns,
// so X-Nexus-Write-LSN reflects writes performed BY THIS REQUEST — not
// unrelated concurrent writes that bumped the pool-wide watermark.
//
// Contract: only ReplicaPool.Exec should call Record. Reading via Load
// is safe for any caller, including middleware and tests.
type WriteLSNRecorder struct {
	lsn atomic.Uint64
}

// Load returns the highest LSN recorded during the current request, or 0
// if no write went through ReplicaPool.Exec on this context.
func (r *WriteLSNRecorder) Load() uint64 {
	return r.lsn.Load()
}

// Record advances the per-request LSN monotonically. Called by
// ReplicaPool.Exec after a successful write. Exposed (rather than
// kept package-private) so tests can simulate writes without needing
// a live Postgres replica.
func (r *WriteLSNRecorder) Record(lsn uint64) {
	for {
		old := r.lsn.Load()
		if lsn <= old {
			return
		}
		if r.lsn.CompareAndSwap(old, lsn) {
			return
		}
	}
}

type writeLSNRecorderKey struct{}

// WithWriteLSNRecorder attaches a fresh WriteLSNRecorder to ctx and
// returns the recorder so the caller (typically the RYOW middleware)
// can read it after the handler chain returns.
func WithWriteLSNRecorder(ctx context.Context) (context.Context, *WriteLSNRecorder) {
	rec := &WriteLSNRecorder{}
	return context.WithValue(ctx, writeLSNRecorderKey{}, rec), rec
}

// WriteLSNRecorderFromContext returns the recorder attached to ctx by
// WithWriteLSNRecorder, or nil if none. ReplicaPool.Exec calls this to
// publish the post-write LSN; tests use it to simulate a write without
// a live Postgres.
func WriteLSNRecorderFromContext(ctx context.Context) *WriteLSNRecorder {
	rec, _ := ctx.Value(writeLSNRecorderKey{}).(*WriteLSNRecorder)
	return rec
}

// writeLSNRecorderFromContext is kept as the internal lookup so that
// existing call sites (currently just Exec) don't shift to the exported
// name — keeps the call-graph for the recorder narrow.
func writeLSNRecorderFromContext(ctx context.Context) *WriteLSNRecorder {
	return WriteLSNRecorderFromContext(ctx)
}
