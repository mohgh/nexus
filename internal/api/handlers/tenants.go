package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/mohgh/nexus/internal/domain"
)

// ListTenants returns all tenants.
// GET /api/v1/tenants
func ListTenants(repo domain.TenantRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenants, err := repo.List(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list tenants")
			return
		}
		writeJSON(w, http.StatusOK, tenants)
	}
}

// CreateTenant creates a new tenant.
// POST /api/v1/tenants
// Body: {"name": "Acme", "plan": "pro"}
func CreateTenant(repo domain.TenantRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name string `json:"name"`
			Plan string `json:"plan"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if req.Name == "" {
			writeError(w, http.StatusBadRequest, "name is required")
			return
		}
		if req.Plan == "" {
			req.Plan = "free"
		}

		now := time.Now().UTC()
		t := &domain.Tenant{
			ID:        uuid.New().String(),
			Name:      req.Name,
			Plan:      req.Plan,
			CreatedAt: now,
			UpdatedAt: now,
		}

		if err := repo.Create(r.Context(), t); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to create tenant")
			return
		}
		writeJSON(w, http.StatusCreated, t)
	}
}

// GetTenant returns a single tenant by ID.
// GET /api/v1/tenants/{tenantID}
func GetTenant(repo domain.TenantRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "tenantID")

		t, err := repo.Get(r.Context(), id)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				writeError(w, http.StatusNotFound, "tenant not found")
				return
			}
			writeError(w, http.StatusInternalServerError, "failed to get tenant")
			return
		}
		writeJSON(w, http.StatusOK, t)
	}
}

// writeJSON encodes v as JSON and writes it with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

// APIError is the structured error response for all Nexus API errors.
// Every error includes a machine-readable code, a human-readable message,
// and the request ID for correlation with server logs.
type APIError struct {
	Error     string `json:"error"`
	Code      string `json:"code"`
	RequestID string `json:"request_id,omitempty"`
}

// writeError writes a structured JSON error body with an error code
// derived from the HTTP status and the request ID from the middleware.
func writeError(w http.ResponseWriter, status int, msg string) {
	reqID := w.Header().Get("X-Request-Id")
	writeJSON(w, status, APIError{
		Error:     msg,
		Code:      errorCode(status),
		RequestID: reqID,
	})
}

func errorCode(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "BAD_REQUEST"
	case http.StatusNotFound:
		return "NOT_FOUND"
	case http.StatusConflict:
		return "CONFLICT"
	case http.StatusServiceUnavailable:
		return "SERVICE_UNAVAILABLE"
	default:
		return "INTERNAL_ERROR"
	}
}
