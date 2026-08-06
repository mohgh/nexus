package handlers

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
)

// WorkflowStarter can start a named workflow and return its run ID.
// Implemented by the Temporal client wrapper in billing/temporal.
type WorkflowStarter interface {
	StartBillingWorkflow(ctx context.Context, input BillingInput) (runID string, err error)
}

// BillingInput mirrors temporal.ChargeInput — kept separate to avoid
// importing the billing package from the handlers package.
type BillingInput struct {
	TenantID       string `json:"tenant_id"`
	IdempotencyKey string `json:"idempotency_key"`
	AmountCents    int64  `json:"amount_cents"`
	Currency       string `json:"currency"`
	Description    string `json:"description"`
}

// Charge starts a Temporal billing workflow for a tenant.
// POST /api/v1/billing/charge
//
// Ch08 teaching points:
//  1. Idempotency key — client provides it; server deduplicates.
//     If the client retries, the same workflow run is returned.
//  2. The HTTP response returns immediately after workflow START.
//     The actual charge happens asynchronously in Temporal.
//     Use Temporal UI (localhost:8233) to observe the workflow progress.
func Charge(starter WorkflowStarter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input BillingInput
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if input.TenantID == "" || input.AmountCents <= 0 {
			writeError(w, http.StatusBadRequest, "tenant_id and amount_cents > 0 are required")
			return
		}
		if input.IdempotencyKey == "" {
			input.IdempotencyKey = uuid.New().String()
		}
		if input.Currency == "" {
			input.Currency = "USD"
		}

		runID, err := starter.StartBillingWorkflow(r.Context(), input)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to start billing workflow")
			return
		}

		writeJSON(w, http.StatusAccepted, map[string]any{
			"status":          "accepted",
			"workflow_run_id": runID,
			"idempotency_key": input.IdempotencyKey,
			"message":         "charge is processing — poll Temporal UI for status",
		})
	}
}
