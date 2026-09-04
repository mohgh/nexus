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

// ─── Regressions for "the masker destroys non-PII business data" ─────────
//
// Masking used to run the regexes over the serialized JSON text.
// That made every non-PII value in the document a candidate for a
// regex written for a different purpose, and the loose phone pattern
// duly matched hyphenated business identifiers. Because the GDPR
// anonymisation pass writes the masked bytes back over events and
// events_store — the source of truth, no rollback — a false positive
// was permanent data loss, not a cosmetic bug.

// TestMasker_MasksPIIInsideJSONValues pins the behaviour that must
// survive the JSON-aware rewrite: real PII in a value, at any depth
// and inside arrays, is still redacted.
func TestMasker_MasksPIIInsideJSONValues(t *testing.T) {
	m := pii.NewMasker()

	payload := json.RawMessage(`{
		"user": {"contact": "alice@example.com", "mobile": "+1 415 555 0132"},
		"session": {"client_ip": "203.0.113.7"},
		"tags": ["support", "bob@test.co"],
		"note": "call (415) 555-0132 back"
	}`)

	masked, cats := m.Mask(payload)

	assert.ElementsMatch(t,
		[]pii.Category{pii.CategoryEmail, pii.CategoryIP, pii.CategoryPhone},
		cats,
	)

	out := string(masked)
	for _, secret := range []string{
		"alice@example.com", "bob@test.co",
		"203.0.113.7",
		"+1 415 555 0132", "(415) 555-0132",
	} {
		assert.NotContains(t, out, secret, "PII value should have been redacted")
	}
	assert.True(t, json.Valid(masked), "masked payload must remain valid JSON: %s", out)
}

// TestMasker_LeavesBusinessIdentifiersAlone is the headline
// regression. `order_ref: "2024-0007711"` matched the old phone
// regex and was overwritten with [REDACTED] inside events_store.
// None of these values is PII and none may be touched.
func TestMasker_LeavesBusinessIdentifiersAlone(t *testing.T) {
	m := pii.NewMasker()

	payload := json.RawMessage(`{"order_ref":"2024-0007711","invoice":"INV-2024-0001",` +
		`"sku":"SKU-99-1234","occurred_at":"2024-06-01T12:30:45Z","amount_cents":249900,` +
		`"version":"1.24.0","page":"/checkout"}`)

	masked, cats := m.Mask(payload)

	assert.Nil(t, cats, "no PII category should fire on a payload of business identifiers")

	out := string(masked)
	for _, keep := range []string{
		"2024-0007711", "INV-2024-0001", "SKU-99-1234",
		"2024-06-01T12:30:45Z", "249900", "1.24.0", "/checkout",
	} {
		assert.Contains(t, out, keep, "non-PII business value was destroyed")
	}
	assert.NotContains(t, out, "[REDACTED]")
	assert.True(t, json.Valid(masked), "masked payload must remain valid JSON: %s", out)
}

// TestMasker_DetectIgnoresBusinessIdentifiers keeps Detect and Mask
// in agreement — cmd/pii-scanner reports on Detect, so a false
// positive there sends an operator hunting for PII that isn't there.
func TestMasker_DetectIgnoresBusinessIdentifiers(t *testing.T) {
	m := pii.NewMasker()

	cats := m.Detect(json.RawMessage(`{"order_ref":"2024-0007711","invoice":"INV-2024-0001"}`))
	assert.Nil(t, cats)
}

// TestMasker_DoesNotMangleJSONKeys covers the other half of "operate
// on the tree, not the text": a key that happens to look like PII is
// schema, not user data, and renaming it breaks every consumer.
func TestMasker_DoesNotMangleJSONKeys(t *testing.T) {
	m := pii.NewMasker()

	payload := json.RawMessage(`{"alice@example.com":"seat-4","10.0.0.1":{"role":"admin"}}`)

	masked, _ := m.Mask(payload)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(masked, &decoded),
		"masked payload must remain valid JSON: %s", masked)

	assert.Contains(t, decoded, "alice@example.com", "JSON keys must not be rewritten")
	assert.Contains(t, decoded, "10.0.0.1", "JSON keys must not be rewritten")
}

// TestMasker_PreservesNonStringValues guards the type contract: the
// placeholder is a string, so numbers, booleans and null are out of
// scope entirely. A numeric order id can never become "[REDACTED]".
func TestMasker_PreservesNonStringValues(t *testing.T) {
	m := pii.NewMasker()

	payload := json.RawMessage(`{"email":"alice@example.com","order_id":20240007711,` +
		`"value":1.5,"paid":true,"refunded":null}`)

	masked, cats := m.Mask(payload)
	require.Equal(t, []pii.Category{pii.CategoryEmail}, cats)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(masked, &decoded))

	assert.Equal(t, "[REDACTED]", decoded["email"])
	assert.Contains(t, string(masked), "20240007711", "numeric ids keep their literal form")
	assert.IsType(t, true, decoded["paid"])
	assert.Nil(t, decoded["refunded"])
}

// TestMasker_MaskStringStillHandlesNonJSON pins the string path that
// middleware.PIIDetect depends on — it passes "path?query", which is
// not JSON and must still be masked.
func TestMasker_MaskStringStillHandlesNonJSON(t *testing.T) {
	m := pii.NewMasker()

	const url = "/api/v1/users?email=alice@example.com&ip=203.0.113.7"

	assert.Equal(t,
		"/api/v1/users?email=[REDACTED]&ip=[REDACTED]",
		m.MaskString(url),
	)
	assert.Equal(t,
		[]pii.Category{pii.CategoryEmail, pii.CategoryIP},
		m.Detect(json.RawMessage(url)),
		"Detect must fall back to raw-text scanning for non-JSON input",
	)
}

// TestMasker_CleanPayloadIsReturnedUnchanged: a payload with no PII
// is not rewritten at all, so anonymisation passes cannot churn
// clean rows (and cannot reformat stored JSON as a side effect).
func TestMasker_CleanPayloadIsReturnedUnchanged(t *testing.T) {
	m := pii.NewMasker()

	payload := json.RawMessage(`{"page":"/pricing","duration_ms":142}`)
	masked, cats := m.Mask(payload)

	assert.Nil(t, cats)
	assert.JSONEq(t, string(payload), string(masked))
	assert.Equal(t, string(payload), string(masked))
}

// TestMasker_MaskIsIdempotent: re-running the anonymisation pass
// over an already-masked row must find nothing new, otherwise the
// "rows anonymised" counter grows on every pass.
func TestMasker_MaskIsIdempotent(t *testing.T) {
	m := pii.NewMasker()

	once, cats := m.Mask(json.RawMessage(`{"email":"alice@example.com","ip":"10.0.0.1"}`))
	require.Len(t, cats, 2)

	twice, again := m.Mask(once)
	assert.Nil(t, again, "a masked payload must not re-trigger detection")
	assert.Equal(t, string(once), string(twice))
}

// TestMasker_PhoneBoundaries documents exactly where the tightened
// phone pattern draws the line between a phone number and an ID.
func TestMasker_PhoneBoundaries(t *testing.T) {
	m := pii.NewMaskerFor(pii.CategoryPhone)

	phones := []string{
		"+1 415 555 0132",
		"+1-415-555-0132",
		"+442079460958",
		"(415) 555-0132",
		"415-555-0132",
		"415.555.0132",
	}
	for _, s := range phones {
		t.Run("phone/"+s, func(t *testing.T) {
			assert.Equal(t, []pii.Category{pii.CategoryPhone},
				m.Detect(json.RawMessage(`{"v":`+quote(s)+`}`)),
				"%q should be detected as a phone number", s)
		})
	}

	notPhones := []string{
		"2024-0007711",
		"INV-2024-0001",
		"SKU-99-1234",
		"2024-06-01",
		"order 4155550132",
		"1.24.0",
	}
	for _, s := range notPhones {
		t.Run("not-phone/"+s, func(t *testing.T) {
			assert.Nil(t, m.Detect(json.RawMessage(`{"v":`+quote(s)+`}`)),
				"%q is a business identifier, not a phone number", s)
		})
	}
}

func quote(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}
