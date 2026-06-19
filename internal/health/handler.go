package health

import (
	"encoding/json"
	"github.com/daddevv/ollama-tap/internal/metrics"
	"net/http"
)

type HealthHandler struct {
	metrics *metrics.Metrics
}

func NewHealthHandler(m *metrics.Metrics) *HealthHandler {
	return &HealthHandler{metrics: m}
}

func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/_tap/stats" {
		h.handleStats(w, r)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{"ok": true})
}

func (h *HealthHandler) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	snap := h.metrics.Snapshot()
	json.NewEncoder(w).Encode(snap)
}
