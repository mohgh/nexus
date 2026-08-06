package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/mohgh/nexus/internal/stream"
	"github.com/redis/go-redis/v9"
)

// LiveStatsConfig governs the SSE handler's pacing and which window
// it reports on.
type LiveStatsConfig struct {
	// Window is the aggregator whose bucket the handler reports. Use
	// time.Minute for "live dashboard" cadence, time.Hour for slower
	// trend views. Defaults to time.Minute.
	Window time.Duration

	// Tick is how often a new SSE event is emitted. Defaults to 1s.
	Tick time.Duration
}

// LiveStats streams the current event-time bucket's count and sum for
// (tenant, event_type) over Server-Sent Events.
//
//	GET /api/v1/tenants/{tenantID}/live-stats?event_type=page_view
//
// The response is a long-lived HTTP/1.1 connection emitting
// `data: {json}\n\n` frames at the configured Tick interval. The
// handler stops when the client disconnects (request context
// cancels) or the server shuts down.
//
// SSE semantics:
//
//	- text/event-stream content type with no buffering
//	- each frame is a complete JSON object with timestamp + count + sum
//	- no retry/event-id semantics — clients should just reconnect on
//	  drop, which EventSource does automatically
//
// The handler reads directly from the Redis hash the stream
// processor populates. There is no separate "subscribe" channel —
// SSE here is a *server-side poll* rendered as a push to the client.
// That's a simplification: a fully event-driven version would tail
// Kafka or subscribe to Redis keyspace notifications, but for the
// chapter's "live dashboard" lesson the poll-from-server cadence is
// honest and avoids extra moving parts.
func LiveStats(client *redis.Client, cfg LiveStatsConfig) http.HandlerFunc {
	if cfg.Window <= 0 {
		cfg.Window = time.Minute
	}
	if cfg.Tick <= 0 {
		cfg.Tick = 1 * time.Second
	}

	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := chi.URLParam(r, "tenantID")
		eventType := r.URL.Query().Get("event_type")
		if tenantID == "" || eventType == "" {
			writeError(w, http.StatusBadRequest, "tenantID path param and event_type query param are required")
			return
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			writeError(w, http.StatusInternalServerError, "streaming unsupported on this connection")
			return
		}

		// SSE headers. Cache-Control on the response and on intermediate
		// proxies is critical — without no-cache and no-buffering hints,
		// frames can sit in a proxy buffer instead of reaching the client.
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering, no-op otherwise
		w.WriteHeader(http.StatusOK)

		// Emit the first frame immediately so the client sees something
		// without waiting for the first tick.
		if err := emitFrame(r.Context(), w, flusher, client, tenantID, eventType, cfg.Window); err != nil {
			return
		}

		ticker := time.NewTicker(cfg.Tick)
		defer ticker.Stop()

		for {
			select {
			case <-r.Context().Done():
				// Client disconnected. Returning ends the handler;
				// net/http closes the response writer.
				return
			case <-ticker.C:
				if err := emitFrame(r.Context(), w, flusher, client, tenantID, eventType, cfg.Window); err != nil {
					return
				}
			}
		}
	}
}

// liveStatsFrame is the JSON shape of each SSE event.
//
// `WindowStart` is derived from the stream processor's persisted
// watermark (event-time), not from wall-clock — see emitFrame for
// why. `BasedOn` makes the source explicit so a UI that wants to
// show "live (event-time)" vs "live (no events yet)" can render
// accordingly.
type liveStatsFrame struct {
	TenantID    string     `json:"tenant_id"`
	EventType   string     `json:"event_type"`
	WindowLabel string     `json:"window"`
	WindowStart time.Time  `json:"window_start"`
	Count       int64      `json:"event_count"`
	SumValue    float64    `json:"sum_value"`
	BasedOn     string     `json:"based_on"`            // "watermark" or "wall_clock"
	Watermark   *time.Time `json:"watermark,omitempty"` // omitted on wall_clock frames
	ObservedAt  time.Time  `json:"observed_at"`
}

// emitFrame reads the current bucket from Redis and writes one SSE
// event. The bucket is chosen by the stream processor's persisted
// event-time watermark, NOT by wall-clock — these can disagree
// around bucket boundaries and especially when traffic is bursty
// with mixed-OccurredAt events. Wall-clock is used only as a
// fallback when no watermark has been written yet (no events ever
// processed for this duration).
//
// Returns an error if writing fails (typically client gone).
func emitFrame(ctx context.Context, w http.ResponseWriter, flusher http.Flusher,
	client *redis.Client, tenantID, eventType string, window time.Duration) error {

	now := time.Now().UTC()

	// Pick the bucket using the stream processor's watermark when
	// available, falling back to wall-clock when no events have been
	// processed yet (the first frame on a brand-new aggregator).
	basedOn := "watermark"
	watermark, wmErr := stream.PersistedWatermarkForDuration(ctx, client, window)
	var bucketTime time.Time
	if wmErr != nil || watermark.IsZero() {
		bucketTime = now
		basedOn = "wall_clock"
	} else {
		bucketTime = watermark
	}
	bucketStart := bucketTime.Truncate(window)

	key := "window:" + windowLabel(window) + ":" + tenantID + ":" + eventType + ":" +
		strconv.FormatInt(bucketStart.Unix(), 10)

	res, err := client.HMGet(ctx, key, "count", "sum").Result()
	var count int64
	var sum float64
	if err == nil {
		if v, ok := res[0].(string); ok {
			_, _ = fmt.Sscanf(v, "%d", &count)
		}
		if v, ok := res[1].(string); ok {
			_, _ = fmt.Sscanf(v, "%f", &sum)
		}
	}

	frame := liveStatsFrame{
		TenantID:    tenantID,
		EventType:   eventType,
		WindowLabel: windowLabel(window),
		WindowStart: bucketStart,
		Count:       count,
		SumValue:    sum,
		BasedOn:     basedOn,
		ObservedAt:  now,
	}
	if !watermark.IsZero() {
		frame.Watermark = &watermark
	}
	body, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", body); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

// windowLabel mirrors the label format used by the stream package's
// key builder — kept in sync deliberately. If you change one, change
// the other. (Duplicated rather than imported to keep handlers from
// taking a dep on the stream package, which is a transitively heavy
// graph through kafka-go.)
func windowLabel(d time.Duration) string {
	switch d {
	case time.Minute:
		return "1m"
	case time.Hour:
		return "1h"
	default:
		return d.String()
	}
}
