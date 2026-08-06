package protobuf_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/mohgh/nexus/internal/domain"
	"github.com/mohgh/nexus/internal/encoding/protobuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Ch05: round-trip test proves Marshal + Unmarshal are inverses.
// Also verifies that adding an unknown field on the wire doesn't break
// the decoder (forward-compatibility).
func TestMarshalUnmarshal_RoundTrip(t *testing.T) {
	original := &domain.Event{
		ID:         "evt-123",
		TenantID:   "tenant-abc",
		EventType:  "page_view",
		Payload:    json.RawMessage(`{"url":"/pricing","duration_ms":142}`),
		Value:      99.9,
		OccurredAt: time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC),
	}

	b, err := protobuf.Marshal(original)
	require.NoError(t, err)
	assert.NotEmpty(t, b)

	decoded := &domain.Event{}
	require.NoError(t, protobuf.Unmarshal(b, decoded))

	assert.Equal(t, original.ID, decoded.ID)
	assert.Equal(t, original.TenantID, decoded.TenantID)
	assert.Equal(t, original.EventType, decoded.EventType)
	assert.JSONEq(t, string(original.Payload), string(decoded.Payload))
	assert.InDelta(t, original.Value, decoded.Value, 1e-9)
	assert.Equal(t, original.OccurredAt.UnixNano(), decoded.OccurredAt.UnixNano())
}

func TestUnmarshal_UnknownFieldSkipped(t *testing.T) {
	original := &domain.Event{
		ID:        "evt-456",
		TenantID:  "t1",
		EventType: "click",
		Payload:   json.RawMessage(`{}`),
	}

	b, err := protobuf.Marshal(original)
	require.NoError(t, err)

	// Append a fake field 9 (varint wire type) — simulates a new optional
	// field added by a future producer. The old decoder must ignore it.
	// field 9, wire type 0 → tag = (9 << 3) | 0 = 72 = 0x48
	b = append(b, 0x48) // tag: field 9, wire type 0 (varint)
	b = append(b, 0x01) // value: 1

	decoded := &domain.Event{}
	require.NoError(t, protobuf.Unmarshal(b, decoded), "unknown fields must not cause errors")
	assert.Equal(t, original.ID, decoded.ID)
}
