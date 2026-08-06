package chaos

import (
	"context"

	"github.com/mohgh/nexus/internal/domain"
)

// Publishable is the minimal contract chaos.Publisher cares about.
// stream.EventProducer satisfies it; tests can supply a fake.
// Defined here (rather than importing handlers.Publisher) so the
// chaos package has no upstream dependencies on api/handlers.
type Publishable interface {
	Publish(ctx context.Context, e *domain.Event) error
}

// Publisher wraps any Publishable with the drop_publish toggle.
// When the toggle is set, Publish returns nil immediately — pretending
// success — without invoking the inner publisher. This produces the
// chapter's asymmetric-partial-failure demo: the DB write commits
// and the API returns 201, but the downstream Kafka consumer sees
// nothing. Toggle it off and the next call publishes normally.
//
// We deliberately return nil rather than ErrPublishDropped because
// the handler treats publish failures as non-fatal (fire-and-forget,
// per IngestEvent's comment). Returning an error would log noise on
// every request; the silent-drop matches the failure mode being
// demonstrated.
type Publisher struct {
	profile *Profile
	inner   Publishable
}

// NewPublisher wraps inner with the chaos drop_publish toggle.
// nil profile is supported (no-op wrapper) so the constructor can
// be unconditional in main.go.
func NewPublisher(profile *Profile, inner Publishable) *Publisher {
	return &Publisher{profile: profile, inner: inner}
}

func (p *Publisher) Publish(ctx context.Context, e *domain.Event) error {
	if p.profile != nil && p.profile.ShouldDropPublish() {
		return nil
	}
	return p.inner.Publish(ctx, e)
}
