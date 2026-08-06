package handlers

import (
	"context"
	"net/http"
)

// LeaderInspector reports the current leadership state for a role,
// including the fencing token issued at acquisition.
type LeaderInspector interface {
	IsLeader() bool
	FencingToken() int64
	CurrentLeader(ctx context.Context) (string, error)
	CurrentFencingToken(ctx context.Context) (int64, error)
}

// LeaderStatus reports whether this node is the leader for the
// outbox role, plus the fencing tokens involved.
//
// GET /api/v1/leader
//
// Ch10: run two instances of Nexus and hit this endpoint on each.
// Only one returns is_leader=true. Kill the leader and watch the
// other take over after the TTL expires (15s). Each acquisition
// gets a strictly-higher fencing_token — the global counter is what
// downstream resources use to reject writes from a stale leader
// that hasn't realised it's no longer in charge.
func LeaderStatus(inspector LeaderInspector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		current, err := inspector.CurrentLeader(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to read leader")
			return
		}
		globalToken, err := inspector.CurrentFencingToken(r.Context())
		if err != nil {
			// Soft-fail: the global token is informational; missing
			// it shouldn't drop the rest of the status.
			globalToken = 0
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"is_leader":             inspector.IsLeader(),
			"current_leader":        current,
			"my_fencing_token":      inspector.FencingToken(),
			"global_fencing_token":  globalToken,
		})
	}
}
