package config

import (
	"net/url"
	"os"
)

type Config struct {
	ListenAddr          string
	Upstream            *url.URL
	LogDir              string
	CaptureRequests     bool
	CaptureResponses    bool
	CaptureStreamChunks bool
}

func FromEnv() (*Config, error) {
	addr := os.Getenv("OLLAMA_TAP_LISTEN_ADDR")
	if addr == "" {
		addr = ":11435"
	}
	upstreamStr := os.Getenv("OLLAMA_TAP_UPSTREAM")
	if upstreamStr == "" {
		return nil, &ConfigError{"OLLAMA_TAP_UPSTREAM environment variable is required"}
	}
	upstream, err := url.Parse(upstreamStr)
	if err != nil {
		return nil, &ConfigError{err.Error()}
	}
	if upstream.Scheme == "" {
		upstream.Scheme = "http"
	}
	logDir := os.Getenv("OLLAMA_TAP_LOG_DIR")
	if logDir == "" {
		logDir = "/tmp/ollama-tap"
	}
	captureReqs := os.Getenv("OLLAMA_TAP_CAPTURE_REQUESTS") == "true"
	captureResps := os.Getenv("OLLAMA_TAP_CAPTURE_RESPONSES") == "true"
	captureChunks := os.Getenv("OLLAMA_TAP_CAPTURE_STREAM_CHUNKS") == "true"
	return &Config{
		ListenAddr:          addr,
		Upstream:            upstream,
		LogDir:              logDir,
		CaptureRequests:     captureReqs,
		CaptureResponses:    captureResps,
		CaptureStreamChunks: captureChunks,
	}, nil
}

type ConfigError struct {
	Message string
}

func (e *ConfigError) Error() string { return e.Message }
