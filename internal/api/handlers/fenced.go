package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/mohgh/nexus/internal/election"
)

// FencedResource is the downstream half of the Ch10 fencing-token
// pattern, exposed as an HTTP endpoint so Ch09's chaos demos can
// drive it. Implemented by *election.FencedResource.
type FencedResource interface {
	Apply(token int64, value string) error
	Highest() int64
	Applied() []election.AppliedWrite
}

// FencedApply attempts to apply a write under a caller-supplied
// fencing token.
//
//	POST /api/v1/protected
//	{"token": N, "value": "..."}
//
// The endpoint is the working demonstration of why fencing tokens
// matter: hit `/api/v1/leader` for the current `global_fencing_token`,
// POST a write here with that token, then have a "ghost leader"
// (a process that's been paused, lost leadership, and woken up
// with its old token) try to POST the same. The second POST gets
// `409 Conflict` with code `FENCED_OFF` because the resource has
// already accepted a higher token.
//
// Together with the chaos endpoint, this lets a student drive the
// canonical DDIA Ch9 scenario end-to-end without writing a single
// goroutine — the chapter's "this is why fencing tokens exist"
// proof made interactive.
func FencedApply(r FencedResource) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		var body struct {
			Token int64  `json:"token"`
			Value string `json:"value"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if body.Token <= 0 {
			writeError(w, http.StatusBadRequest, "token must be a positive integer")
			return
		}

		if err := r.Apply(body.Token, body.Value); err != nil {
			if errors.Is(err, election.ErrFencedOff) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"error":"token is older than the highest applied","code":"FENCED_OFF","highest":` +
					itoa(r.Highest()) + `}`))
				return
			}
			writeError(w, http.StatusInternalServerError, "apply failed")
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"status":  "applied",
			"token":   body.Token,
			"highest": r.Highest(),
		})
	}
}

// FencedState exposes the current downstream state — highest token
// applied and the sequence of accepted writes.
//
//	GET /api/v1/protected
//
// Useful in tandem with FencedApply for the chapter's stale-leader
// demo: after running through the ghost-leader scenario, hit GET
// /api/v1/protected to see only the new leader's writes landed.
func FencedState(r FencedResource) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"highest": r.Highest(),
			"applied": r.Applied(),
		})
	}
}

// itoa avoids importing strconv just for one int64 → string in the
// inline 409 body. Kept local to this file.
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// _ = context.Background reference kept so future evolutions can
// thread a request context through without adding the import.
var _ = context.Background
