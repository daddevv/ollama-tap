package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daddevv/ollama-tap/internal/metrics"
)

func TestDashboardHandler(t *testing.T) {
	m := metrics.New()
	tracker := metrics.NewModelUsageTracker()
	store := metrics.NewRingStore(m, tracker)
	defer store.Stop()

	mux := http.NewServeMux()
	RegisterHandlers(mux, store, tracker, m)

	req := httptest.NewRequest("GET", "/_tap/dashboard", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}

	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Errorf("Content-Type: got %q, want text/html", ct)
	}
}

func TestDashboardHandler_NotFound(t *testing.T) {
	m := metrics.New()
	tracker := metrics.NewModelUsageTracker()
	store := metrics.NewRingStore(m, tracker)
	defer store.Stop()

	mux := http.NewServeMux()
	RegisterHandlers(mux, store, tracker, m)

	// Sub-path of dashboard should not be found (only exact matches)
	req := httptest.NewRequest("GET", "/_tap/dashboard/subpath", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestSnapshotEndpoint(t *testing.T) {
	m := metrics.New()
	tracker := metrics.NewModelUsageTracker()
	defer tracker.Stop()
	store := metrics.NewRingStore(m, tracker)
	defer store.Stop()

	// Trigger a tick so snapshot has data
	store.Tick()
	tracker.Record("qwen", 10, 25)
	m.IncrementActive()

	mux := http.NewServeMux()
	RegisterHandlers(mux, store, tracker, m)

	req := httptest.NewRequest("GET", "/_tap/dashboard/api/snapshot", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json decode: %v", err)
	}

	requiredKeys := []string{"timestamp", "active_connections", "total_requests"}
	for _, key := range requiredKeys {
		if _, ok := body[key]; !ok {
			t.Errorf("missing key %q in snapshot response", key)
		}
	}
	if body["active_connections"].(float64) != 1 {
		t.Fatalf("active_connections: got %v, want 1", body["active_connections"])
	}
	if body["active_streaming_connections"].(float64) != 0 {
		t.Fatalf("active_streaming_connections: got %v, want 0", body["active_streaming_connections"])
	}
	if body["failed_requests"].(float64) != 0 {
		t.Fatalf("failed_requests: got %v, want 0", body["failed_requests"])
	}
	if body["total_prompt_tokens"].(float64) != 10 {
		t.Fatalf("total_prompt_tokens: got %v, want 10", body["total_prompt_tokens"])
	}
	if body["total_completion_tokens"].(float64) != 25 {
		t.Fatalf("total_completion_tokens: got %v, want 25", body["total_completion_tokens"])
	}
}

func TestHistoryEndpoint(t *testing.T) {
	m := metrics.New()
	tracker := metrics.NewModelUsageTracker()
	defer tracker.Stop()
	store := metrics.NewRingStore(m, tracker)
	defer store.Stop()

	// Trigger a tick
	store.Tick()
	m.RecordRequest(10*time.Millisecond, true)
	tracker.Record("qwen", 5, 8)
	store.Tick()

	mux := http.NewServeMux()
	RegisterHandlers(mux, store, tracker, m)

	req := httptest.NewRequest("GET", "/_tap/dashboard/api/history?minutes=10", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}

	var body []map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json decode: %v", err)
	}

	// Should be a valid JSON array (may be empty if no ticks yet)
	for i, entry := range body {
		for _, key := range []string{"label", "timestamp", "total_reqs_delta"} {
			if _, ok := entry[key]; !ok {
				t.Errorf("entry %d: missing key %q", i, key)
			}
		}
	}
	if len(body) == 0 {
		t.Fatal("expected at least one history entry")
	}
	last := body[len(body)-1]
	if last["prompt_tokens_delta"].(float64) != 5 {
		t.Fatalf("prompt_tokens_delta: got %v, want 5", last["prompt_tokens_delta"])
	}
	if last["completion_tokens_delta"].(float64) != 8 {
		t.Fatalf("completion_tokens_delta: got %v, want 8", last["completion_tokens_delta"])
	}
}

func TestHistoryEndpoint_DefaultMinutes(t *testing.T) {
	m := metrics.New()
	tracker := metrics.NewModelUsageTracker()
	store := metrics.NewRingStore(m, tracker)
	defer store.Stop()

	store.Tick()

	mux := http.NewServeMux()
	RegisterHandlers(mux, store, tracker, m)

	// No minutes param — should default to 60
	req := httptest.NewRequest("GET", "/_tap/dashboard/api/history", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestHistoryEndpoint_InvalidPoints(t *testing.T) {
	m := metrics.New()
	tracker := metrics.NewModelUsageTracker()
	defer tracker.Stop()
	store := metrics.NewRingStore(m, tracker)
	defer store.Stop()

	store.Tick()

	mux := http.NewServeMux()
	RegisterHandlers(mux, store, tracker, m)

	req := httptest.NewRequest("GET", "/_tap/dashboard/api/history?points=9999", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHistoryEndpoint_InvalidMinutes(t *testing.T) {
	m := metrics.New()
	tracker := metrics.NewModelUsageTracker()
	store := metrics.NewRingStore(m, tracker)
	defer store.Stop()

	store.Tick()

	mux := http.NewServeMux()
	RegisterHandlers(mux, store, tracker, m)

	// Invalid value should return 400 (validated input)
	req := httptest.NewRequest("GET", "/_tap/dashboard/api/history?minutes=-5", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestModelsEndpoint(t *testing.T) {
	m := metrics.New()
	tracker := metrics.NewModelUsageTracker()
	store := metrics.NewRingStore(m, tracker)
	defer store.Stop()

	// Record some usage
	tracker.Record("gpt-4", 100, 200)
	tracker.Record("llama3", 50, 100)

	mux := http.NewServeMux()
	RegisterHandlers(mux, store, tracker, m)

	req := httptest.NewRequest("GET", "/_tap/dashboard/api/models", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}

	var body map[string]map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json decode: %v", err)
	}

	if _, ok := body["gpt-4"]; !ok {
		t.Error("missing gpt-4 in models response")
	}
	if _, ok := body["llama3"]; !ok {
		t.Error("missing llama3 in models response")
	}

	gpt4 := body["gpt-4"]
	if float64(gpt4["prompt_tokens"].(float64)) != 100 {
		t.Errorf("gpt-4 prompt_tokens: got %v, want 100", gpt4["prompt_tokens"])
	}
}

func TestModelsEndpoint_Empty(t *testing.T) {
	m := metrics.New()
	tracker := metrics.NewModelUsageTracker()
	store := metrics.NewRingStore(m, tracker)
	defer store.Stop()

	mux := http.NewServeMux()
	RegisterHandlers(mux, store, tracker, m)

	req := httptest.NewRequest("GET", "/_tap/dashboard/api/models", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}

	var body map[string]map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json decode: %v", err)
	}

	// Should be empty map, not null
	if len(body) != 0 {
		t.Errorf("expected empty map, got %d entries", len(body))
	}
}
