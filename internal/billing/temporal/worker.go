package temporal

import (
	"fmt"

	"github.com/mohgh/nexus/internal/domain"
	pgstore "github.com/mohgh/nexus/internal/storage/postgres"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// NewClient opens a Temporal gRPC connection to the given host:port.
// Default Temporal server address is "localhost:7233".
func NewClient(hostPort string) (client.Client, error) {
	c, err := client.Dial(client.Options{
		HostPort: hostPort,
	})
	if err != nil {
		return nil, fmt.Errorf("temporal: dial %s: %w", hostPort, err)
	}
	return c, nil
}

// StartWorker registers the BillingWorkflow and its activities, then starts
// polling the TaskQueue. Run this in a goroutine from main.go.
//
// The worker survives individual activity failures — Temporal retries according
// to the RetryPolicy in workflow.go. The worker itself can be restarted freely.
func StartWorker(c client.Client, tenants domain.TenantRepository, billing domain.BillingRepository, pool *pgstore.Pool) worker.Worker {
	w := worker.New(c, TaskQueue, worker.Options{})

	// Register workflow — Temporal maps the function name to the workflow type.
	w.RegisterWorkflow(BillingWorkflow)

	// Register activities — inject dependencies here, not via globals.
	// pool is needed for the cross-table transaction in RecordCharge
	// (deduct tenant_credits + insert billing_records atomically).
	//
	// No PublishChargeEvent registration: see activities.go for why
	// it was removed. The outbox worker handles publication.
	acts := NewActivities(tenants, billing, pool)
	w.RegisterActivity(acts.ValidateCharge)
	w.RegisterActivity(acts.RecordCharge)

	return w
}
