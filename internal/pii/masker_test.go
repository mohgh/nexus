package pii_test

import (
	"encoding/json"
	"testing"

	"github.com/mohgh/nexus/internal/pii"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMasker_Detect(t *testing.T) {
	m := pii.NewMasker()

	tests := []struct {
		name    string
		payload string
		want    []pii.Category
	}{
		{
			name:    "email",
			payload: `{"user_email": "alice@example.com"}`,
			want:    []pii.Category{pii.CategoryEmail},
		},
		{
			name:    "ip address",
			payload: `{"client_ip": "192.168.1.42"}`,
			want:    []pii.Category{pii.CategoryIP},
		},
		{
			name:    "multiple categories",
			payload: `{"email": "bob@test.co", "ip": "10.0.0.1"}`,
			want:    []pii.Category{pii.CategoryEmail, pii.CategoryIP},
		},
		{
			name:    "no PII",
			payload: `{"page": "/pricing", "duration_ms": 142}`,
			want:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cats := m.Detect(json.RawMessage(tt.payload))
			assert.Equal(t, tt.want, cats)
		})
	}
}

func TestMasker_Mask(t *testing.T) {
	m := pii.NewMasker()

	payload := json.RawMessage(`{"email": "alice@example.com", "ip": "10.0.0.1", "page": "/home"}`)
	masked, cats := m.Mask(payload)

	require.Len(t, cats, 2)

	// Email and IP should be redacted, page should remain.
	assert.Contains(t, string(masked), "[REDACTED]")
	assert.NotContains(t, string(masked), "alice@example.com")
	assert.NotContains(t, string(masked), "10.0.0.1")
	assert.Contains(t, string(masked), "/home")
}

func TestMasker_SelectiveCategories(t *testing.T) {
	m := pii.NewMaskerFor(pii.CategoryEmail)

	payload := json.RawMessage(`{"email": "x@y.com", "ip": "1.2.3.4"}`)
	masked, cats := m.Mask(payload)

	// Only email should be masked.
	assert.Equal(t, []pii.Category{pii.CategoryEmail}, cats)
	assert.NotContains(t, string(masked), "x@y.com")
	assert.Contains(t, string(masked), "1.2.3.4") // IP not masked
}
