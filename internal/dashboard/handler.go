// Package dashboard provides the live observability dashboard for ollama-tap.
package dashboard

import (
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"

	"github.com/daddevv/ollama-tap/internal/metrics"
)

//go:embed assets
var assets embed.FS

const defaultHistoryMinutes = 1440 // 24 hours
const defaultHistoryPoints = 288
const maxHistoryPoints = 720

func cacheControl(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=5, public")
		next(w, r)
	}
}

// RegisterHandlers registers all dashboard endpoints on the given mux.
func RegisterHandlers(mux *http.ServeMux, store *metrics.RingStore, tracker *metrics.ModelUsageTracker, m *metrics.Metrics) {
	mux.HandleFunc("/_tap/dashboard", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_tap/dashboard" && r.URL.Path != "/_tap/dashboard/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		data, err := assets.ReadFile("assets/index.html")
		if err != nil {
			http.Error(w, "dashboard asset not found", http.StatusInternalServerError)
			return
		}
		w.Write(data)
	})

	mux.HandleFunc("/_tap/dashboard/api/snapshot", cacheControl(func(w http.ResponseWriter, r *http.Request) {
		snap := store.Snapshot()
		st := m.Snapshot()

		totals := tracker.Totals()

		out := map[string]interface{}{
			"timestamp":                 snap["timestamp"],
			"active_connections":        snap["active_connections"],
			"active_streaming_connections": st.ActiveStreaming,
			"total_requests":            st.TotalRequests,
			"failed_requests":           st.Failures,
			"streaming_connections":     st.StreamingCount,
			"non_streaming_connections": st.NonStreamingCount,
			"uptime":                    st.Uptime,
			// Token totals across all models.
			"total_prompt_tokens":       totals.PromptTokens,
			"total_completion_tokens":   totals.CompletionTokens,
			"total_tokens":              totals.TotalTokens,
		}
		respondJSON(w, out)
	}))

	mux.HandleFunc("/_tap/dashboard/api/history", cacheControl(func(w http.ResponseWriter, r *http.Request) {
		minutesStr := r.URL.Query().Get("minutes")
		pointsStr := r.URL.Query().Get("points")
		var minutes int = defaultHistoryMinutes
		var points int = defaultHistoryPoints
		if minutesStr != "" {
			val, err := strconv.Atoi(minutesStr)
			if err != nil {
				http.Error(w, fmt.Sprintf("invalid minutes value: %q", minutesStr), http.StatusBadRequest)
				return
			}
			if val <= 0 || val > 1440 {
				http.Error(w, "minutes must be between 1 and 1440", http.StatusBadRequest)
				return
			}
			minutes = val
		}
		if pointsStr != "" {
			val, err := strconv.Atoi(pointsStr)
			if err != nil {
				http.Error(w, fmt.Sprintf("invalid points value: %q", pointsStr), http.StatusBadRequest)
				return
			}
			if val <= 0 || val > maxHistoryPoints {
				http.Error(w, fmt.Sprintf("points must be between 1 and %d", maxHistoryPoints), http.StatusBadRequest)
				return
			}
			points = val
		}
		history := store.HistorySeries(minutes, points)
		respondJSON(w, history)
	}))

	mux.HandleFunc("/_tap/dashboard/api/models", cacheControl(func(w http.ResponseWriter, r *http.Request) {
		data := tracker.Snapshot()
		respondJSON(w, data)
	}))

	var dashboardReady sync.Once
	dashboardReady.Do(func() { log.Println("dashboard: enabled — visit /_tap/dashboard") })
}

func respondJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("dashboard: JSON encode error: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}
