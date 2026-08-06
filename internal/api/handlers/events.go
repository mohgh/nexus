package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/mohgh/nexus/internal/auth"
	"github.com/mohgh/nexus/internal/domain"
)

// Publisher is the write side of the Kafka producer.
// Kept as an interface so the handler can be tested without Kafka running.
type Publisher interface {
	Publish(ctx context.Context, e *domain.Event) error
}

// IngestEvent creates a new event for the authenticated tenant.
// POST /api/v1/events
//
// Body: {"event_type":"page_view","payload":{…},"value":0}
//
// Tenant isolation (override model): the tenant is taken from the API key
// in context, NOT from the request body. Any tenant_id a client sends is
// ignored — a caller can only ever write events for its own tenant.
//
// Ch05: if a publisher is provided, the event is also sent to Kafka as
// Protobuf after the PostgreSQL write. This is the dual-write pattern —
// Ch12 evolves this into a CDC pipeline to remove the dual write.
func IngestEvent(repo domain.EventRepository, pub Publisher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := auth.TenantFromContext(r.Context())
		if tenantID == "" {
			writeError(w, http.StatusForbidden, "a tenant-scoped API key is required to ingest events")
			return
		}

		var req struct {
			EventType string          `json:"event_type"`
			Payload   json.RawMessage `json:"payload"`
			Value     float64         `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if req.EventType == "" {
			writeError(w, http.StatusBadRequest, "event_type is required")
			return
		}
		if len(req.Payload) == 0 {
			req.Payload = json.RawMessage(`{}`)
		}

		e := &domain.Event{
			ID:         uuid.New().String(),
			TenantID:   tenantID,
			EventType:  req.EventType,
			Payload:    req.Payload,
			Value:      req.Value,
			OccurredAt: time.Now().UTC(),
		}
		if err := repo.Create(r.Context(), e); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to ingest event")
			return
		}

		// Ch05: publish to Kafka after the DB write.
		// Fire-and-forget: a publish failure does not fail the HTTP request —
		// the event is durable in PostgreSQL. Ch12 removes this dual-write
		// by using Debezium CDC to stream from the WAL instead.
		if pub != nil {
			if err := pub.Publish(r.Context(), e); err != nil {
				// Log but don't fail — event is already persisted in Postgres.
				_ = err
			}
		}

		writeJSON(w, http.StatusCreated, e)
	}
}

// IngestEventBatch accepts an array of events in one request.
// POST /api/v1/events/batch
//
// Body: {"events":[{"event_type":"…","payload":{…},"value":0,"occurred_at":"…"}, …]}
//
// Tenant isolation (override model): the tenant comes from the API key in
// context, never from the body. This is what makes batched ingest safe —
// earlier designs that took tenant_id from the request turned a single
// CORS-allowed batch into a cross-tenant forgery primitive. Now every event
// in the batch is written for the authenticated tenant, full stop.
//
// Why a batch endpoint exists: browser SDKs buffer events client-side and
// flush periodically (every N events or every T seconds). One HTTP
// round-trip per event would dominate page-load budgets and make sendBeacon
// useless (each beacon counts against a per-page byte budget). The batch
// shape lets the SDK ship dozens of events in a single beacon on unload.
//
// occurred_at is optional and client-supplied. SDKs stamp it at capture()
// time so an event queued offline and flushed an hour later still records
// when it actually happened — not when it reached the server. Missing or
// out-of-window values fall back to server time (see clock-skew clamp).
//
// Partial-success behaviour: each event is validated and written
// independently. The response reports per-event status; the HTTP code is
// 207 (Multi-Status) when any event failed, 201 otherwise. A single bad
// event in a batch of 100 never drops the other 99 — important because SDKs
// retry the whole batch on non-2xx and we'd otherwise loop forever on one
// poison event.
//
// Publish path: per-event repo.Create runs inside the request goroutine
// (so the request timeout actually applies to persistence), but Kafka
// publishes run in a background goroutine started AFTER the response is
// written. Background publish + Postgres-as-source-of-truth keeps the
// request fast and the dedup contract honest. Ch08's outbox is the
// principled long-term answer; this is its course-appendix shortcut.
func IngestEventBatch(repo domain.EventRepository, pub Publisher) http.HandlerFunc {
	const (
		maxBatchSize = 500
		// maxBatchBytes caps the request body to keep batches under the
		// Idempotency middleware's 1 MiB fingerprint cap (so retries get
		// cached responses) AND under the browser fetch(..., {keepalive:true})
		// ~64 KiB total budget that applies on unload paths. 256 KiB is
		// comfortably inside the idempotency cap and large enough for
		// hundreds of typical analytics events.
		maxBatchBytes = 256 * 1024
	)

	type eventIn struct {
		EventType  string          `json:"event_type"`
		Payload    json.RawMessage `json:"payload"`
		Value      float64         `json:"value"`
		OccurredAt *time.Time      `json:"occurred_at,omitempty"`
	}
	type resultOut struct {
		Index  int    `json:"index"`
		ID     string `json:"id,omitempty"`
		Status int    `json:"status"`
		Code   string `json:"code,omitempty"`
		Error  string `json:"error,omitempty"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := auth.TenantFromContext(r.Context())
		if tenantID == "" {
			writeError(w, http.StatusForbidden, "a tenant-scoped API key is required to ingest events")
			return
		}

		// Hard byte cap. MaxBytesReader makes Decode return an error the
		// moment the body crosses the limit, so we never buffer a 50 MiB
		// JSON blob in memory just to reject it.
		r.Body = http.MaxBytesReader(w, r.Body, maxBatchBytes)

		var req struct {
			Events []eventIn `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			// MaxBytesError -> 413, malformed JSON -> 400.
			var mbe *http.MaxBytesError
			if errors.As(err, &mbe) {
				writeError(w, http.StatusRequestEntityTooLarge,
					"batch exceeds maximum of "+strconv.Itoa(maxBatchBytes)+" bytes")
				return
			}
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if len(req.Events) == 0 {
			writeError(w, http.StatusBadRequest, "events array is required and non-empty")
			return
		}
		if len(req.Events) > maxBatchSize {
			writeError(w, http.StatusRequestEntityTooLarge,
				"batch exceeds maximum of "+strconv.Itoa(maxBatchSize)+" events")
			return
		}

		now := time.Now().UTC()
		results := make([]resultOut, len(req.Events))
		toPublish := make([]*domain.Event, 0, len(req.Events))
		anyFailed := false

		for i, in := range req.Events {
			if in.EventType == "" {
				results[i] = resultOut{Index: i, Status: http.StatusBadRequest,
					Code: "EVENT_TYPE_REQUIRED", Error: "event_type is required"}
				anyFailed = true
				continue
			}
			payload := in.Payload
			if len(payload) == 0 {
				payload = json.RawMessage(`{}`)
			}
			occurred := now
			// Clock-skew clamp: accept client timestamps from up to 24h in
			// the past to 1h in the future. Outside that window the SDK clock
			// is implausible and we'd rather have server time than poison
			// Ch12's event-time bucketing.
			if in.OccurredAt != nil && !in.OccurredAt.IsZero() {
				if delta := now.Sub(*in.OccurredAt); delta > -time.Hour && delta < 24*time.Hour {
					occurred = in.OccurredAt.UTC()
				}
			}

			e := &domain.Event{
				ID:         uuid.New().String(),
				TenantID:   tenantID,
				EventType:  in.EventType,
				Payload:    payload,
				Value:      in.Value,
				OccurredAt: occurred,
			}
			if err := repo.Create(r.Context(), e); err != nil {
				results[i] = resultOut{Index: i, Status: http.StatusInternalServerError,
					Code: "PERSIST_FAILED", Error: "persist failed"}
				anyFailed = true
				continue
			}
			toPublish = append(toPublish, e)
			results[i] = resultOut{Index: i, ID: e.ID, Status: http.StatusCreated}
		}

		status := http.StatusCreated
		if anyFailed {
			status = http.StatusMultiStatus
		}
		writeJSON(w, status, map[string]any{
			"accepted": len(req.Events),
			"results":  results,
		})

		// Publish AFTER responding. Postgres is the source of truth; a
		// publish failure is recoverable by the outbox / CDC path (Ch08,
		// Ch12). The detached context means we keep publishing even if the
		// HTTP client disconnects between response and the last event.
		if pub != nil && len(toPublish) > 0 {
			go func(events []*domain.Event) {
				ctx := context.Background()
				for _, e := range events {
					_ = pub.Publish(ctx, e)
				}
			}(toPublish)
		}
	}
}

// ListEvents returns recent events for the authenticated tenant.
// GET /api/v1/events?limit=100
//
// The tenant is taken from the API key in context — a ?tenant_id query
// param, if present, is ignored.
func ListEvents(repo domain.EventRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := auth.TenantFromContext(r.Context())
		if tenantID == "" {
			writeError(w, http.StatusForbidden, "a tenant-scoped API key is required")
			return
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))

		events, err := repo.ListByTenant(r.Context(), tenantID, limit)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list events")
			return
		}
		writeJSON(w, http.StatusOK, events)
	}
}

// SearchEvents does a JSONB payload search for the authenticated tenant.
// GET /api/v1/events/search?q=…&limit=50
//
// Ch03: demonstrates PostgreSQL JSONB + pg_trgm search. The tenant is taken
// from the API key in context, so one tenant can never search another's
// events.
func SearchEvents(repo domain.EventRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := auth.TenantFromContext(r.Context())
		if tenantID == "" {
			writeError(w, http.StatusForbidden, "a tenant-scoped API key is required")
			return
		}
		query := r.URL.Query().Get("q")
		if query == "" {
			writeError(w, http.StatusBadRequest, "q is required")
			return
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))

		events, err := repo.Search(r.Context(), tenantID, query, limit)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "search failed")
			return
		}
		writeJSON(w, http.StatusOK, events)
	}
}
