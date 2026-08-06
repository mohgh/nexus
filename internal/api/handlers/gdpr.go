package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/mohgh/nexus/internal/gdpr"
)

// DataExporter can export all data for a tenant (GDPR Article 20 — data portability).
type DataExporter interface {
	ExportTenantData(ctx context.Context, tenantID string) (any, error)
}

// DataEraser can erase all data for a tenant (GDPR Article 17 — right to erasure).
type DataEraser interface {
	EraseTenantData(ctx context.Context, tenantID string) error
}

// DataAnonymiser replaces PII in tenant events with redaction
// placeholders, preserving the row count for audit purposes.
// Lighter alternative to full erasure when "this user existed and
// did N things" is the question and the identities aren't.
type DataAnonymiser interface {
	AnonymiseTenantEvents(ctx context.Context, tenantID string) (gdpr.AnonymiseResult, error)
}

// ConsentManager manages consent records.
//
// The version parameter on Grant binds the consent to a specific
// privacy-policy version. Pass 0 to default to 1 — single-policy
// setups don't need to track versions.
type ConsentManager interface {
	Grant(ctx context.Context, tenantID, purpose string, version int) error
	Revoke(ctx context.Context, tenantID, purpose string) error
	ListByTenant(ctx context.Context, tenantID string) (any, error)
}

// ExportData returns all data associated with a tenant in JSON format.
// GET /api/v1/gdpr/{tenantID}/export
//
// Ch14: this is the "data portability" endpoint. GDPR Article 20 requires
// that data subjects can download their data in a machine-readable format.
func ExportData(exporter DataExporter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := chi.URLParam(r, "tenantID")
		if tenantID == "" {
			writeError(w, http.StatusBadRequest, "tenantID is required")
			return
		}

		data, err := exporter.ExportTenantData(r.Context(), tenantID)
		if err != nil {
			if errors.Is(err, gdpr.ErrTenantNotFound) {
				writeError(w, http.StatusNotFound, "tenant not found")
				return
			}
			writeError(w, http.StatusInternalServerError, "export failed")
			return
		}
		writeJSON(w, http.StatusOK, data)
	}
}

// ErasureRequest deletes all data for a tenant.
// POST /api/v1/gdpr/{tenantID}/erasure
//
// Ch14: GDPR Article 17 — "right to be forgotten." This deletes events,
// billing records, and all derived data for the tenant. The audit log
// entry for the erasure itself is retained (legal requirement).
func ErasureRequest(eraser DataEraser) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := chi.URLParam(r, "tenantID")
		if tenantID == "" {
			writeError(w, http.StatusBadRequest, "tenantID is required")
			return
		}

		if err := eraser.EraseTenantData(r.Context(), tenantID); err != nil {
			if errors.Is(err, gdpr.ErrTenantNotFound) {
				writeError(w, http.StatusNotFound, "tenant not found")
				return
			}
			// Anything else is genuine failure — and a partial
			// erasure is the worst outcome (some data deleted, the
			// caller thinks none was). Surfacing 500 rather than
			// 200 with a misleading body matches the audit's
			// "erasure returns success only if all data deleted"
			// claim.
			writeError(w, http.StatusInternalServerError, "erasure failed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{
			"status":    "erased",
			"tenant_id": tenantID,
			"message":   "all tenant data has been deleted — audit log retained",
		})
	}
}

// ManageConsent handles consent grant/revoke for a tenant.
// POST /api/v1/gdpr/{tenantID}/consent
// Body: {"purpose": "analytics", "action": "grant"|"revoke"}
func ManageConsent(manager ConsentManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := chi.URLParam(r, "tenantID")
		if tenantID == "" {
			writeError(w, http.StatusBadRequest, "tenantID is required")
			return
		}

		var req struct {
			Purpose string `json:"purpose"`
			Action  string `json:"action"`           // "grant" or "revoke"
			Version int    `json:"version,omitempty"` // privacy-policy version for grant; 0 -> default 1
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if req.Purpose == "" || (req.Action != "grant" && req.Action != "revoke") {
			writeError(w, http.StatusBadRequest, "purpose and action (grant|revoke) are required")
			return
		}

		var err error
		switch req.Action {
		case "grant":
			err = manager.Grant(r.Context(), tenantID, req.Purpose, req.Version)
		case "revoke":
			err = manager.Revoke(r.Context(), tenantID, req.Purpose)
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "consent update failed")
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{
			"status":  req.Action + "ed",
			"purpose": req.Purpose,
		})
	}
}

// Anonymise replaces PII fields in tenant events with [REDACTED]
// and marks the rows pii_erased=true. Preserves row count + event
// type + occurred_at; only the payload is modified.
//
// POST /api/v1/gdpr/{tenantID}/anonymise
func Anonymise(anon DataAnonymiser) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := chi.URLParam(r, "tenantID")
		if tenantID == "" {
			writeError(w, http.StatusBadRequest, "tenantID is required")
			return
		}
		result, err := anon.AnonymiseTenantEvents(r.Context(), tenantID)
		if err != nil {
			if errors.Is(err, gdpr.ErrTenantNotFound) {
				writeError(w, http.StatusNotFound, "tenant not found")
				return
			}
			writeError(w, http.StatusInternalServerError, "anonymisation failed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":                  "anonymised",
			"tenant_id":               tenantID,
			"events_scanned":          result.EventsScanned,
			"events_anonymised":       result.EventsAnonymised,
			"event_store_scanned":     result.EventStoreScanned,
			"event_store_anonymised":  result.EventStoreAnonymised,
		})
	}
}

// ListConsent returns all consent records for a tenant.
// GET /api/v1/gdpr/{tenantID}/consent
func ListConsent(manager ConsentManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := chi.URLParam(r, "tenantID")
		records, err := manager.ListByTenant(r.Context(), tenantID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list consent")
			return
		}
		writeJSON(w, http.StatusOK, records)
	}
}
