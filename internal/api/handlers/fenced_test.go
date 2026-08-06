package handlers_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mohgh/nexus/internal/api/handlers"
	"github.com/mohgh/nexus/internal/election"
)

// TestFencedApply_HappyAndStale walks the canonical DDIA Ch9
// stale-leader scenario through the HTTP surface:
//
//   1. New leader applies with token=5 → 200, highest=5.
//   2. Old leader (still holding token=3 from before its pause)
//      applies → 409 FENCED_OFF, highest unchanged.
//   3. Same new leader retries with the same token=5 → 409
//      FENCED_OFF as well (the contract is strictly increasing).
//
// The 409 body carries the highest token so a client can decide
// whether to fetch a fresh one and retry, or give up.
func TestFencedApply_HappyAndStale(t *testing.T) {
	t.Parallel()
	r := election.NewFencedResource()
	apply := handlers.FencedApply(r)
	state := handlers.FencedState(r)

	// Step 1: token 5 succeeds.
	rr := httptest.NewRecorder()
	apply.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/protected",
		bytes.NewReader([]byte(`{"token":5,"value":"new-leader-write"}`))))
	if rr.Code != http.StatusOK {
		t.Fatalf("token=5 status: got %d, want 200", rr.Code)
	}

	// Step 2: stale token 3 is fenced off.
	rr = httptest.NewRecorder()
	apply.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/protected",
		bytes.NewReader([]byte(`{"token":3,"value":"ghost-leader-write"}`))))
	if rr.Code != http.StatusConflict {
		t.Fatalf("stale token=3 status: got %d, want 409", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "FENCED_OFF") {
		t.Fatalf("response body should carry FENCED_OFF code, got %s", rr.Body.String())
	}

	// Step 3: replaying token=5 also fenced off (strictly
	// increasing — the contract doesn't permit equal tokens).
	rr = httptest.NewRecorder()
	apply.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/protected",
		bytes.NewReader([]byte(`{"token":5,"value":"replay-of-new-leader-write"}`))))
	if rr.Code != http.StatusConflict {
		t.Fatalf("equal token=5 should also 409, got %d", rr.Code)
	}

	// State endpoint confirms the new leader's write landed,
	// nothing else.
	rr = httptest.NewRecorder()
	state.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/protected", nil))
	var resp struct {
		Highest int64                    `json:"highest"`
		Applied []election.AppliedWrite  `json:"applied"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp.Highest != 5 {
		t.Fatalf("highest: got %d, want 5", resp.Highest)
	}
	if len(resp.Applied) != 1 || resp.Applied[0].Value != "new-leader-write" {
		t.Fatalf("only the first new-leader write should be in applied list, got %+v", resp.Applied)
	}
}

// TestFencedApply_RejectsNonPositiveToken: token 0 or negative is
// a caller bug (the real Elector starts tokens at 1), so 400.
func TestFencedApply_RejectsNonPositiveToken(t *testing.T) {
	t.Parallel()
	r := election.NewFencedResource()
	apply := handlers.FencedApply(r)

	for _, body := range []string{`{"token":0,"value":"x"}`, `{"token":-1,"value":"x"}`} {
		rr := httptest.NewRecorder()
		apply.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/protected",
			bytes.NewReader([]byte(body))))
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("token from %s: got %d, want 400", body, rr.Code)
		}
	}
}
