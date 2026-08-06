package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/mohgh/nexus/internal/auth"
)

// APIKeyStore is the admin surface for minting and revoking API keys.
// Implemented by *postgres.APIKeyRepository. Revoke returns
// auth.ErrKeyNotFound when no active key matches.
type APIKeyStore interface {
	Create(ctx context.Context, k *auth.APIKey) error
	Revoke(ctx context.Context, id string) error
}

// CreateAPIKey mints a new key and returns the raw secret EXACTLY ONCE.
// Admin-only (mounted under the admin group).
//
//	POST /api/v1/admin/api-keys
//	Body: {"scope":"tenant"|"admin","tenant_id":"<uuid>","name":"…"}
//
// scope defaults to "tenant". A tenant key requires tenant_id; an admin key
// must omit it. The response's api_key field is the only time the secret is
// ever visible — it is hashed before storage and cannot be retrieved again.
func CreateAPIKey(store APIKeyStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Scope    string `json:"scope"`
			TenantID string `json:"tenant_id"`
			Name     string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		scope := auth.Scope(req.Scope)
		if scope == "" {
			scope = auth.ScopeTenant
		}
		if scope != auth.ScopeTenant && scope != auth.ScopeAdmin {
			writeError(w, http.StatusBadRequest, `scope must be "tenant" or "admin"`)
			return
		}

		raw, key, err := auth.NewKey(scope, req.TenantID, req.Name)
		if err != nil {
			// NewKey enforces the scope/tenant invariant — a bad combination
			// (e.g. tenant key with no tenant_id) is a client error.
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := store.Create(r.Context(), key); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to create api key")
			return
		}

		writeJSON(w, http.StatusCreated, map[string]any{
			"id":         key.ID,
			"api_key":    raw, // shown once — store it now, it cannot be retrieved again
			"scope":      string(key.Scope),
			"tenant_id":  key.TenantID,
			"prefix":     key.Prefix,
			"created_at": key.CreatedAt,
			"warning":    "store this key now; for security it is never retrievable again",
		})
	}
}

// RevokeAPIKey soft-deletes a key by ID. Admin-only.
//
//	DELETE /api/v1/admin/api-keys/{keyID}
func RevokeAPIKey(store APIKeyStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "keyID")
		if id == "" {
			writeError(w, http.StatusBadRequest, "key id is required")
			return
		}
		if err := store.Revoke(r.Context(), id); err != nil {
			if errors.Is(err, auth.ErrKeyNotFound) {
				writeError(w, http.StatusNotFound, "api key not found or already revoked")
				return
			}
			writeError(w, http.StatusInternalServerError, "failed to revoke api key")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{
			"status": "revoked",
			"id":     id,
		})
	}
}
