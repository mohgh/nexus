// Package pii provides PII detection and masking for GDPR compliance.
//
// Ch14 teaching points:
//  1. PII (Personally Identifiable Information) appears in event payloads
//     that Nexus stores as JSONB. Email addresses, IP addresses, user names
//     can all end up in unstructured analytics events.
//  2. Masking replaces PII with a redacted placeholder so the event remains
//     structurally valid but no longer contains identifying data.
//  3. The masker runs as a pre-processor on event payloads before storage
//     (consent-aware: only mask if the tenant hasn't given consent for that
//     data category) or as a post-processor when exporting data.
//  4. Detection uses regex patterns — this is a heuristic, not a guarantee.
//     Production systems use NLP-based PII classifiers or data catalogs.
package pii

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
)

// Category represents a type of PII.
type Category string

const (
	CategoryEmail Category = "email"
	CategoryIP    Category = "ip_address"
	CategoryPhone Category = "phone"
)

// pattern maps each PII category to its detection regex.
//
// The phone pattern is deliberately conservative. Its predecessor
// (`\b\+?[1-9]\d{1,2}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4}\b`)
// matched any long-ish hyphenated run of digits, so business
// identifiers such as an order reference "2024-0007711" were
// classified as phone numbers and permanently overwritten in the
// event store. A phone number now has to look like one:
//
//   - an international number, which must carry a leading "+"; or
//   - a grouped local number, which must carry a separator between
//     the exchange and the line number (415-555-0132, (415) 555-0132).
//
// A bare 10-digit run and a single-hyphen numeric ID are both left
// alone: neither is distinguishable from a business key, and the
// cost of a false positive here is irreversible data loss.
var patterns = map[Category]*regexp.Regexp{
	CategoryEmail: regexp.MustCompile(`[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`),
	CategoryIP:    regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`),
	CategoryPhone: regexp.MustCompile(`\+\d[\d\s.\-()]{6,16}\d|(?:\(\d{3}\)|\b\d{3})[\s.\-]?\d{3}[\s.\-]\d{4}\b`),
}

// placeholder is the replacement string for masked PII.
const placeholder = "[REDACTED]"

// Masker detects and replaces PII in JSON payloads.
type Masker struct {
	// categories lists which PII categories to detect and mask.
	categories []Category
}

// NewMasker creates a masker that detects all PII categories by default.
func NewMasker() *Masker {
	return &Masker{
		categories: []Category{CategoryEmail, CategoryIP, CategoryPhone},
	}
}

// NewMaskerFor creates a masker that only detects the specified categories.
func NewMaskerFor(cats ...Category) *Masker {
	return &Masker{categories: cats}
}

// Detect scans a JSON payload and returns which PII categories were found.
//
// When the input parses as JSON only string *values* are scanned —
// object keys, numbers and booleans are structure, not user data.
// Anything that is not JSON (PIIDetect hands this method a
// "path?query" string) falls back to scanning the raw text.
func (m *Masker) Detect(payload json.RawMessage) []Category {
	hits := map[Category]bool{}
	if tree, ok := decodeJSON(payload); ok {
		m.walk(tree, hits, false)
	} else {
		m.scanString(string(payload), hits, false)
	}
	return m.ordered(hits)
}

// Mask replaces all detected PII in the payload with [REDACTED].
// Returns the masked payload and a list of categories that were masked.
//
// Ch14: this used to run the regexes over the *serialized* JSON,
// which meant the patterns competed with JSON syntax and with every
// non-PII value in the document. A false positive there was not a
// missed detection but a destructive write: the anonymisation pass
// in internal/gdpr writes the result straight back over events and
// events_store, which is the source of truth and has no rollback.
//
// Mask now decodes the payload and walks the tree, rewriting string
// values in place. Keys are never touched, numbers keep their type
// (redacting a number would have to change it to a string), and the
// output is re-encoded, so it is valid JSON by construction. When
// nothing matched the original bytes are returned untouched, so a
// clean payload is never even reformatted.
//
// MaskString keeps the raw-string path for genuinely non-JSON input.
func (m *Masker) Mask(payload json.RawMessage) (json.RawMessage, []Category) {
	hits := map[Category]bool{}

	tree, ok := decodeJSON(payload)
	if !ok {
		// Not JSON — degrade to the plain-text path rather than
		// silently returning the payload unmasked.
		text := m.scanString(string(payload), hits, true)
		return json.RawMessage(text), m.ordered(hits)
	}

	masked := m.walk(tree, hits, true)
	cats := m.ordered(hits)
	if len(cats) == 0 {
		return payload, nil
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(masked); err != nil {
		// Unreachable: the tree came from valid JSON and we only
		// ever replaced strings with strings. Fail closed by
		// reporting nothing masked rather than writing garbage
		// over the caller's row.
		return payload, nil
	}
	return json.RawMessage(bytes.TrimRight(buf.Bytes(), "\n")), cats
}

// ContainsPII returns true if any PII is detected in the payload.
func (m *Masker) ContainsPII(payload json.RawMessage) bool {
	return len(m.Detect(payload)) > 0
}

// MaskString masks PII in a plain string (e.g. a description field
// or a URL + query string). This is the non-JSON path and stays a
// straight regex replacement.
func (m *Masker) MaskString(s string) string {
	return m.scanString(s, nil, true)
}

// IsMasked checks if a string contains the redaction placeholder.
func IsMasked(s string) bool {
	return strings.Contains(s, placeholder)
}

// decodeJSON decodes payload into a generic tree, reporting whether
// it was in fact a single well-formed JSON document. Numbers are
// kept as json.Number so re-encoding preserves their literal form.
func decodeJSON(payload json.RawMessage) (any, bool) {
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, false
	}
	if dec.More() {
		return nil, false // trailing garbage: not a single document
	}
	return v, true
}

// walk recurses through a decoded JSON value. Only string values are
// eligible for masking: object keys are field names (masking them
// would mangle the schema) and numbers/booleans/null cannot hold a
// [REDACTED] placeholder without changing type.
func (m *Masker) walk(v any, hits map[Category]bool, replace bool) any {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			t[k] = m.walk(val, hits, replace)
		}
		return t
	case []any:
		for i, val := range t {
			t[i] = m.walk(val, hits, replace)
		}
		return t
	case string:
		return m.scanString(t, hits, replace)
	default:
		return v
	}
}

// scanString applies each enabled category's regex to s, recording
// which ones matched in hits (when non-nil). With replace=true the
// matches are rewritten to the placeholder and the rewritten string
// is returned; otherwise s comes back unchanged.
func (m *Masker) scanString(s string, hits map[Category]bool, replace bool) string {
	for _, cat := range m.categories {
		p, ok := patterns[cat]
		if !ok || !p.MatchString(s) {
			continue
		}
		if hits != nil {
			hits[cat] = true
		}
		if replace {
			s = p.ReplaceAllString(s, placeholder)
		}
	}
	return s
}

// ordered returns the matched categories in the masker's configured
// order, so callers get a stable list regardless of map iteration.
func (m *Masker) ordered(hits map[Category]bool) []Category {
	var out []Category
	for _, cat := range m.categories {
		if hits[cat] {
			out = append(out, cat)
		}
	}
	return out
}
