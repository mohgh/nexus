// Package temporal implements the Nexus billing workflow using Temporal.
//
// Ch08 teaching point: Temporal gives you durable execution for multi-step
// processes without 2PC. Each activity runs at-least-once; Temporal retries
// on failure with configurable backoff. The workflow state is persisted in
// Temporal's database — a process crash is transparent to the workflow.
//
// The billing workflow has two steps:
//  1. ValidateCharge  — check tenant exists, plan allows the charge.
//                       Business-rule failures (tenant not found, free plan)
//                       are returned as Temporal non-retryable errors so the
//                       workflow fails immediately rather than burning the
//                       RetryPolicy on impossible cases.
//
//  2. RecordCharge    — atomic Postgres transaction: SELECT … FOR UPDATE on
//                       tenant_credits, deduct, INSERT billing_records with
//                       status='completed' and outbox_sent_at=NULL. Either
//                       both rows commit or neither does.
//
// There is intentionally no "publish to Kafka" step in the workflow. The
// outbox worker (internal/billing/outbox) sees the row at outbox_sent_at=NULL
// and publishes it asynchronously, setting outbox_sent_at on success. An
// earlier draft of this workflow called MarkOutboxSent inside the workflow,
// which silently disabled the outbox on the happy path — the worker never
// saw any pending rows because the workflow had already marked them sent.
// That bug is exactly the failure mode the outbox pattern is meant to make
// impossible, so making sure the workflow does NOT touch outbox_sent_at is
// the load-bearing teaching point of this file.
//
// Contrast with 2PC: 2PC requires both participants to be up simultaneously.
// Temporal's approach requires only eventual availability — the workflow
// waits (not blocks) until each step succeeds.
package temporal

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	TaskQueue    = "nexus-billing"
	WorkflowName = "BillingWorkflow"
)

// actRef is a nil *Activities used only to obtain stable method references for
// workflow.ExecuteActivity. Temporal matches activities by fully-qualified
// function name at runtime — using a nil pointer here is the standard pattern.
var actRef *Activities

// ChargeInput is the input to the BillingWorkflow.
type ChargeInput struct {
	TenantID       string `json:"tenant_id"`
	IdempotencyKey string `json:"idempotency_key"`
	AmountCents    int64  `json:"amount_cents"`
	Currency       string `json:"currency"`
	Description    string `json:"description"`
}

// ChargeResult is returned by the BillingWorkflow.
type ChargeResult struct {
	BillingRecordID string `json:"billing_record_id"`
	Status          string `json:"status"`
}

// BillingWorkflow orchestrates a tenant charge.
//
// Retry semantics:
//   - Transient errors (Postgres connection blip, etc.) retry per RetryPolicy.
//   - Business-rule failures (tenant not found, free plan, insufficient
//     credit) are returned as non-retryable from the activity, so Temporal
//     stops immediately rather than burning attempts on an impossible case.
//   - The workflow itself is replayed if the worker crashes between
//     activities — idempotent activities make this safe.
//
// Kafka publication is intentionally NOT a workflow step; see the package
// doc for why.
func BillingWorkflow(ctx workflow.Context, input ChargeInput) (ChargeResult, error) {
	// Activity options: retry with exponential backoff, max 3 attempts.
	// Each activity runs in a separate goroutine on the worker.
	actOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    3,
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, actOpts)

	// Step 1: Validate the charge (read-only, freely retryable on transient
	// failures; non-retryable on business-rule failures).
	if err := workflow.ExecuteActivity(ctx, actRef.ValidateCharge, input).Get(ctx, nil); err != nil {
		return ChargeResult{Status: "failed"}, err
	}

	// Step 2: Atomically deduct credit and record the charge. The activity
	// returns status="completed" because the transaction has committed —
	// the workflow does not need to (and must not) further mutate the row.
	var result ChargeResult
	if err := workflow.ExecuteActivity(ctx, actRef.RecordCharge, input).Get(ctx, &result); err != nil {
		return ChargeResult{Status: "failed"}, err
	}

	return result, nil
}
