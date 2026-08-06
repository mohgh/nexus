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
var patterns = map[Category]*regexp.Regexp{
	CategoryEmail: regexp.MustCompile(`[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`),
	CategoryIP:    regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`),
	CategoryPhone: regexp.MustCompile(`\b\+?[1-9]\d{1,2}[\s\-]?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4}\b`),
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
func (m *Masker) Detect(payload json.RawMessage) []Category {
	text := string(payload)
	var found []Category
	for _, cat := range m.categories {
		if p, ok := patterns[cat]; ok && p.MatchString(text) {
			found = append(found, cat)
		}
	}
	return found
}

// Mask replaces all detected PII in the payload with [REDACTED].
// Returns the masked payload and a list of categories that were masked.
//
// Ch14: this operates on the raw JSON string. A more robust approach would
// walk the JSON tree and mask only specific fields (e.g. "email", "ip"),
// but string replacement is sufficient for the course demo.
func (m *Masker) Mask(payload json.RawMessage) (json.RawMessage, []Category) {
	text := string(payload)
	var masked []Category

	for _, cat := range m.categories {
		p, ok := patterns[cat]
		if !ok {
			continue
		}
		if p.MatchString(text) {
			text = p.ReplaceAllString(text, placeholder)
			masked = append(masked, cat)
		}
	}

	return json.RawMessage(text), masked
}

// ContainsPII returns true if any PII is detected in the payload.
func (m *Masker) ContainsPII(payload json.RawMessage) bool {
	return len(m.Detect(payload)) > 0
}

// MaskString masks PII in a plain string (e.g. a description field).
func (m *Masker) MaskString(s string) string {
	for _, cat := range m.categories {
		if p, ok := patterns[cat]; ok {
			s = p.ReplaceAllString(s, placeholder)
		}
	}
	return s
}

// IsMasked checks if a string contains the redaction placeholder.
func IsMasked(s string) bool {
	return strings.Contains(s, placeholder)
}
