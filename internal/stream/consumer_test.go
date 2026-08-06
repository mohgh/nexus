package stream

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mohgh/nexus/internal/domain"
	"github.com/mohgh/nexus/internal/encoding/protobuf"
	"github.com/segmentio/kafka-go"
)

// fakeReader serves a pre-loaded sequence of messages from
// FetchMessage and records every CommitMessages call so tests can
// assert which offsets were (and were not) committed.
type fakeReader struct {
	mu          sync.Mutex
	pending     []kafka.Message
	cursor      int
	committed   []int64 // offsets committed, in order
	fetchClosed atomic.Bool
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	for {
		r.mu.Lock()
		if r.cursor < len(r.pending) {
			m := r.pending[r.cursor]
			r.cursor++
			r.mu.Unlock()
			return m, nil
		}
		r.mu.Unlock()
		if r.fetchClosed.Load() {
			return kafka.Message{}, context.Canceled
		}
		select {
		case <-ctx.Done():
			return kafka.Message{}, ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range msgs {
		r.committed = append(r.committed, m.Offset)
	}
	return nil
}

func (r *fakeReader) Close() error { return nil }

func (r *fakeReader) commits() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int64, len(r.committed))
	copy(out, r.committed)
	return out
}

// recordingDLQ captures the entries it receives so tests can assert
// what was diverted. The optional failNext channel lets a test
// simulate the DLQ being down: returning a non-nil error must cause
// the consumer to leave the original message uncommitted.
type recordingDLQ struct {
	mu        sync.Mutex
	entries   []DLQEntry
	failTimes int
}

func (d *recordingDLQ) Publish(_ context.Context, entry DLQEntry) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failTimes > 0 {
		d.failTimes--
		return errors.New("DLQ unavailable")
	}
	d.entries = append(d.entries, entry)
	return nil
}

func (d *recordingDLQ) all() []DLQEntry {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]DLQEntry, len(d.entries))
	copy(out, d.entries)
	return out
}

// validProtobufMessage builds a Kafka message whose Value is a valid
// Protobuf-encoded domain.Event. Helpful for the happy-path tests.
func validProtobufMessage(t *testing.T, offset int64, e *domain.Event) kafka.Message {
	t.Helper()
	body, err := protobuf.Marshal(e)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return kafka.Message{
		Topic:     "nexus.events",
		Partition: 0,
		Offset:    offset,
		Key:       []byte(e.TenantID),
		Value:     body,
	}
}

func newEvent(id string) *domain.Event {
	return &domain.Event{
		ID:         id,
		TenantID:   "tenant-" + id,
		EventType:  "page_view",
		Payload:    []byte(`{}`),
		Value:      1.0,
		OccurredAt: time.Now().UTC(),
	}
}

// runForUpTo starts c.Run in a goroutine, waits until the reader has
// served all its messages and the commit list stabilises, then
// cancels and waits for shutdown. Returns the final commit list.
func runForUpTo(t *testing.T, c *EventConsumer, reader *fakeReader, handler EventHandler, timeout time.Duration) []int64 {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = c.Run(ctx, handler)
		close(done)
	}()

	// Wait for the reader to drain.
	deadline := time.After(timeout)
	for {
		reader.mu.Lock()
		drained := reader.cursor >= len(reader.pending)
		reader.mu.Unlock()
		if drained {
			break
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("messages not drained within %v", timeout)
		case <-time.After(5 * time.Millisecond):
		}
	}
	// Give the consumer one more tick to commit / DLQ the last message.
	time.Sleep(50 * time.Millisecond)
	reader.fetchClosed.Store(true)
	cancel()
	<-done

	return reader.commits()
}

// TestConsumer_DoesNotCommitOnHandlerFailureWithoutDLQ is the
// regression test for the original bug. With no DLQ wired AND a
// handler that always fails, the prior implementation still
// committed the offset, silently dropping the event. The fix: with
// no DLQ, we log + commit (acknowledged trade-off documented in
// consumer.go); with a DLQ but a publish failure, we MUST NOT
// commit. The next test covers the with-DLQ path.
//
// This test pins down the no-DLQ behavior: log + commit, as
// documented. If you flip the documented contract you'll need to
// flip this test deliberately rather than have it silently lie.
func TestConsumer_NoDLQ_LogsAndCommitsOnExhaustion(t *testing.T) {
	t.Parallel()

	e := newEvent("e1")
	reader := &fakeReader{pending: []kafka.Message{validProtobufMessage(t, 100, e)}}
	c := newConsumerWithReader(reader, nil /*no DLQ*/, RetryPolicy{
		MaxAttempts: 2,
		BaseDelay:   1 * time.Millisecond,
	}, nil)

	handler := func(context.Context, *domain.Event) error {
		return errors.New("always fails")
	}

	commits := runForUpTo(t, c, reader, handler, 2*time.Second)
	if len(commits) != 1 || commits[0] != 100 {
		t.Fatalf("with no DLQ wired, exhausted message should be log+committed (documented). got commits=%v", commits)
	}
}

// TestConsumer_WithDLQ_DivertsExhaustedMessageThenCommits is the
// canonical happy DLQ path: handler fails, DLQ accepts, offset is
// committed. The message is durably preserved in the DLQ topic for
// human review.
func TestConsumer_WithDLQ_DivertsExhaustedMessageThenCommits(t *testing.T) {
	t.Parallel()

	e := newEvent("e1")
	reader := &fakeReader{pending: []kafka.Message{validProtobufMessage(t, 100, e)}}
	dlq := &recordingDLQ{}
	c := newConsumerWithReader(reader, dlq, RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   1 * time.Millisecond,
	}, nil)

	var attempts atomic.Int32
	handler := func(context.Context, *domain.Event) error {
		attempts.Add(1)
		return errors.New("never recovers")
	}

	commits := runForUpTo(t, c, reader, handler, 2*time.Second)
	if attempts.Load() != 3 {
		t.Fatalf("handler attempts: got %d, want 3 (MaxAttempts)", attempts.Load())
	}
	if len(commits) != 1 || commits[0] != 100 {
		t.Fatalf("after DLQ accepted the message, offset must be committed. got commits=%v", commits)
	}
	entries := dlq.all()
	if len(entries) != 1 {
		t.Fatalf("DLQ entries: got %d, want 1", len(entries))
	}
	if entries[0].Reason != "handler_failed_after_retries" {
		t.Fatalf("DLQ reason: got %q", entries[0].Reason)
	}
	if entries[0].Attempts != 3 {
		t.Fatalf("DLQ attempts: got %d, want 3", entries[0].Attempts)
	}
	if string(entries[0].OriginalKey) != e.TenantID {
		t.Fatalf("DLQ should preserve original key: got %q, want %q", entries[0].OriginalKey, e.TenantID)
	}
}

// TestConsumer_DLQDownLeavesMessageUncommitted is the load-bearing
// safety property: if we couldn't durably divert the message, we
// must NOT commit. Otherwise a transient DLQ outage = lost events.
func TestConsumer_DLQDownLeavesMessageUncommitted(t *testing.T) {
	t.Parallel()

	e := newEvent("e1")
	reader := &fakeReader{pending: []kafka.Message{validProtobufMessage(t, 100, e)}}
	dlq := &recordingDLQ{failTimes: 100} // every Publish fails
	c := newConsumerWithReader(reader, dlq, RetryPolicy{
		MaxAttempts: 2,
		BaseDelay:   1 * time.Millisecond,
	}, nil)

	handler := func(context.Context, *domain.Event) error {
		return errors.New("always fails")
	}

	commits := runForUpTo(t, c, reader, handler, 2*time.Second)
	if len(commits) != 0 {
		t.Fatalf("with DLQ down, the message must remain uncommitted. got commits=%v", commits)
	}
}

// TestConsumer_HandlerSuccessFirstTryCommitsCleanly is the trivial
// happy path. No retries needed; offset committed; no DLQ entries.
func TestConsumer_HandlerSuccessFirstTryCommitsCleanly(t *testing.T) {
	t.Parallel()

	e1, e2, e3 := newEvent("e1"), newEvent("e2"), newEvent("e3")
	reader := &fakeReader{pending: []kafka.Message{
		validProtobufMessage(t, 10, e1),
		validProtobufMessage(t, 11, e2),
		validProtobufMessage(t, 12, e3),
	}}
	dlq := &recordingDLQ{}
	c := newConsumerWithReader(reader, dlq, RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   1 * time.Millisecond,
	}, nil)

	handler := func(context.Context, *domain.Event) error { return nil }

	commits := runForUpTo(t, c, reader, handler, 2*time.Second)
	if len(commits) != 3 {
		t.Fatalf("commits: got %d, want 3 — expected one commit per processed message", len(commits))
	}
	if len(dlq.all()) != 0 {
		t.Fatalf("DLQ should be empty on happy path, got %d entries", len(dlq.all()))
	}
}

// TestConsumer_TransientFailureSucceedsOnRetry shows that a handler
// which fails once and then succeeds gets the message committed
// exactly once, no DLQ involvement.
func TestConsumer_TransientFailureSucceedsOnRetry(t *testing.T) {
	t.Parallel()

	e := newEvent("e1")
	reader := &fakeReader{pending: []kafka.Message{validProtobufMessage(t, 10, e)}}
	dlq := &recordingDLQ{}
	c := newConsumerWithReader(reader, dlq, RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   1 * time.Millisecond,
	}, nil)

	var calls atomic.Int32
	handler := func(context.Context, *domain.Event) error {
		if calls.Add(1) < 2 {
			return errors.New("transient")
		}
		return nil
	}

	commits := runForUpTo(t, c, reader, handler, 2*time.Second)
	if len(commits) != 1 || commits[0] != 10 {
		t.Fatalf("commits: got %v, want [10]", commits)
	}
	if len(dlq.all()) != 0 {
		t.Fatalf("DLQ should be empty when retry succeeded, got %d entries", len(dlq.all()))
	}
	if calls.Load() != 2 {
		t.Fatalf("handler calls: got %d, want 2 (fail once, succeed)", calls.Load())
	}
}

// TestConsumer_MalformedMessageGoesStraightToDLQ verifies that a
// message we couldn't even decode is sent to the DLQ on the first
// failure — no retries, since retrying a poison message can't help.
func TestConsumer_MalformedMessageGoesStraightToDLQ(t *testing.T) {
	t.Parallel()

	bad := kafka.Message{
		Topic: "nexus.events", Partition: 0, Offset: 50,
		Key:   []byte("tenant-x"),
		Value: []byte("not-a-valid-protobuf"),
	}
	reader := &fakeReader{pending: []kafka.Message{bad}}
	dlq := &recordingDLQ{}
	c := newConsumerWithReader(reader, dlq, RetryPolicy{
		MaxAttempts: 5, // would burn 5 attempts if the consumer wrongly retried decode failures
		BaseDelay:   1 * time.Millisecond,
	}, nil)

	var handlerCalls atomic.Int32
	handler := func(context.Context, *domain.Event) error {
		handlerCalls.Add(1)
		return nil
	}

	commits := runForUpTo(t, c, reader, handler, 2*time.Second)
	if handlerCalls.Load() != 0 {
		t.Fatalf("handler must never be called for an undecodable message, got %d calls", handlerCalls.Load())
	}
	if len(commits) != 1 || commits[0] != 50 {
		t.Fatalf("commits: got %v, want [50] (committed after DLQ accepted the poison message)", commits)
	}
	entries := dlq.all()
	if len(entries) != 1 || entries[0].Reason != "unmarshal_failed" {
		t.Fatalf("DLQ reason: got %+v, want unmarshal_failed", entries)
	}
}

// TestRetryPolicy_BackoffIsExponentialAndCapped is a small unit test
// for the backoff math — the consumer itself uses small delays in
// tests, but production runs with longer delays where the cap matters.
func TestRetryPolicy_BackoffIsExponentialAndCapped(t *testing.T) {
	t.Parallel()

	p := RetryPolicy{
		BaseDelay: 100 * time.Millisecond,
		MaxDelay:  1 * time.Second,
	}
	cases := []struct {
		attempt  int
		expected time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, 1 * time.Second}, // capped
		{6, 1 * time.Second}, // capped
	}
	for _, c := range cases {
		got := p.backoff(c.attempt)
		if got != c.expected {
			t.Fatalf("backoff(%d): got %v, want %v", c.attempt, got, c.expected)
		}
	}
}
