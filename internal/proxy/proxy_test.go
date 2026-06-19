package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/openai/ollama-tap/internal/config"
	"github.com/openai/ollama-tap/internal/metrics"
)

func newTestProxy(t *testing.T) (*Proxy, *httptest.Server) {
	cfg := &config.Config{
		ListenAddr:          ":0",
		Upstream:            mustParse("http://127.0.0.1:0"),
		CaptureRequests:     true,
		CaptureResponses:    true,
		CaptureStreamChunks: true,
		LogDir:              t.TempDir(),
	}
	m := metrics.New()
	p, err := New(cfg, m)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "chat") {
			w.Write([]byte(`{"model":"qwen3.6:latest","done":true,"eval_count":5}`))
		} else if r.URL.Path == "/api/version" {
			w.Write([]byte(`{"version":"0.5.0"}`))
		} else {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]any{
				"id":      "test-model",
				"object":  "model",
				"created": 1234567890,
			})
		}
	}))

	p.cfg.Upstream, _ = parseUpstream(server.URL)
	return p, server
}

func mustParse(s string) *url.URL {
	u, _ := url.Parse(s)
	return u
}

func TestProxyVersionForwarding(t *testing.T) {
	p, server := newTestProxy(t)
	defer server.Close()

	req := httptest.NewRequest("GET", "/api/version", nil)
	w := httptest.NewRecorder()
	p.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}

	var body map[string]any
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["version"] != "0.5.0" {
		t.Errorf("got version %v", body["version"])
	}
}

func TestProxyModelsForwarding(t *testing.T) {
	p, server := newTestProxy(t)
	defer server.Close()

	req := httptest.NewRequest("GET", "/v1/models", nil)
	w := httptest.NewRecorder()
	p.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}

	var body map[string]any
	json.Unmarshal(w.Body.Bytes(), &body)
	if _, ok := body["id"]; !ok {
		t.Error("expected id field in response")
	}
}

func TestProxyChatForwarding(t *testing.T) {
	p, server := newTestProxy(t)
	defer server.Close()

	req := httptest.NewRequest("POST", "/api/chat", strings.NewReader(`{"model":"qwen3.6","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	p.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}

	var body map[string]any
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["done"] != true {
		t.Errorf("expected done=true in response")
	}
}

func TestProxyXForwardedHost(t *testing.T) {
	p, server := newTestProxy(t)
	defer server.Close()

	req := httptest.NewRequest("GET", "/v1/models", nil)
	req.RemoteAddr = "192.168.1.1:12345"
	w := httptest.NewRecorder()

	p.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func parseUpstream(s string) (*url.URL, error) {
	return url.Parse(s)
}
