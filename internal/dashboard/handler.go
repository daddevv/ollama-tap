// Package dashboard provides the live observability dashboard for ollama-tap.
package dashboard

import (
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/daddevv/ollama-tap/internal/metrics"
)

//go:embed assets
var assets embed.FS

const defaultHistoryMinutes = 60

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

	mux.HandleFunc("/_tap/dashboard/api/snapshot", func(w http.ResponseWriter, r *http.Request) {
		snap := store.Snapshot()
		st := m.Snapshot()

		// Sum token totals from all models.
		var totalPromptTokens, totalCompletionTokens int64
		trackerSnapshot := tracker.Snapshot()
		for _, u := range trackerSnapshot {
			totalPromptTokens += u.PromptTokens
			totalCompletionTokens += u.CompletionTokens
		}

		out := map[string]interface{}{
			"timestamp":                 snap["timestamp"],
			"active_connections":        snap["active_connections"],
			"request_count":             st.TotalRequests,
			"total_requests":            st.TotalRequests,
			"streaming_connections":     st.StreamingCount,
			"non_streaming_connections": st.NonStreamingCount,
			"uptime":                    st.Uptime,
			// Token totals across all models.
			"total_prompt_tokens":       totalPromptTokens,
			"total_completion_tokens":   totalCompletionTokens,
			"total_tokens":              totalPromptTokens + totalCompletionTokens,
		}
		respondJSON(w, out)
	})

	mux.HandleFunc("/_tap/dashboard/api/history", func(w http.ResponseWriter, r *http.Request) {
		minutesStr := r.URL.Query().Get("minutes")
		var minutes int = defaultHistoryMinutes
		if minutesStr != "" {
			fmt.Sscanf(minutesStr, "%d", &minutes)
			if minutes <= 0 || minutes > 1440 {
				minutes = defaultHistoryMinutes
			}
		}
		history := store.History(minutes)
		respondJSON(w, history)
	})

	mux.HandleFunc("/_tap/dashboard/api/models", func(w http.ResponseWriter, r *http.Request) {
		data := tracker.Snapshot()
		respondJSON(w, data)
	})

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
