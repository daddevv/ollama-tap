package config

import (
	"os"
	"testing"
)

func TestFromEnv(t *testing.T) {
	os.Setenv("OLLAMA_TAP_UPSTREAM", "http://192.168.0.13:11434")
	defer os.Unsetenv("OLLAMA_TAP_UPSTREAM")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ListenAddr != ":11435" {
		t.Errorf("listen_addr = %q, want :11435", cfg.ListenAddr)
	}
	if cfg.Upstream.Host != "192.168.0.13:11434" {
		t.Errorf("upstream host = %q, want 192.168.0.13:11434", cfg.Upstream.Host)
	}

	if !cfg.CaptureRequests && cfg.CaptureResponses && !cfg.CaptureStreamChunks {
		t.Error("expected defaults (all false)")
	}

	// Test capture flags
	os.Setenv("OLLAMA_TAP_CAPTURE_REQUESTS", "true")
	os.Setenv("OLLAMA_TAP_CAPTURE_RESPONSES", "true")
	os.Setenv("OLLAMA_TAP_CAPTURE_STREAM_CHUNKS", "true")
	cfg2, _ := FromEnv()
	if !cfg2.CaptureRequests || !cfg2.CaptureResponses || !cfg2.CaptureStreamChunks {
		t.Error("expected all capture flags true")
	}

	// Test custom listen addr
	os.Setenv("OLLAMA_TAP_LISTEN_ADDR", ":9999")
	cfg3, _ := FromEnv()
	if cfg3.ListenAddr != ":9999" {
		t.Errorf("listen_addr = %q, want :9999", cfg3.ListenAddr)
	}

	// Test missing upstream
	os.Unsetenv("OLLAMA_TAP_UPSTREAM")
	_, err = FromEnv()
	if err == nil {
		t.Error("expected error for missing OLLAMA_TAP_UPSTREAM")
	}
}
