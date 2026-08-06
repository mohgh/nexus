package temporal

import (
	"context"
	"fmt"

	"github.com/mohgh/nexus/internal/api/handlers"
	"go.temporal.io/sdk/client"
)

// Starter implements handlers.WorkflowStarter using a Temporal client.
type Starter struct {
	client client.Client
}

func NewStarter(c client.Client) *Starter {
	return &Starter{client: c}
}

// Compile-time assertion.
var _ handlers.WorkflowStarter = (*Starter)(nil)

// StartBillingWorkflow submits a BillingWorkflow execution to Temporal.
// Using the idempotency_key as the workflow ID ensures that a client retry
// for the same charge starts the same workflow run — not a duplicate.
func (s *Starter) StartBillingWorkflow(ctx context.Context, input handlers.BillingInput) (string, error) {
	opts := client.StartWorkflowOptions{
		// Workflow ID = idempotency key → retrying with the same key
		// returns the existing workflow, not a new one.
		ID:        fmt.Sprintf("billing-%s-%s", input.TenantID, input.IdempotencyKey),
		TaskQueue: TaskQueue,
	}

	run, err := s.client.ExecuteWorkflow(ctx, opts, BillingWorkflow, ChargeInput{
		TenantID:       input.TenantID,
		IdempotencyKey: input.IdempotencyKey,
		AmountCents:    input.AmountCents,
		Currency:       input.Currency,
		Description:    input.Description,
	})
	if err != nil {
		return "", fmt.Errorf("temporal: start billing workflow: %w", err)
	}
	return run.GetRunID(), nil
}
