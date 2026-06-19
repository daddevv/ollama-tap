package config

import (
	"flag"
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

func RegisterFlags() {
	listenAddr := flag.String("listen", defaultEnvOr("OLLAMA_TAP_LISTEN_ADDR", ":11435"), "address to listen on")
	upstreamURL := flag.String("upstream", defaultEnvOr("OLLAMA_TAP_UPSTREAM", ""), "ollama upstream URL (required)")
	logDir := flag.String("log-dir", defaultEnvOr("OLLAMA_TAP_LOG_DIR", "/tmp/ollama-tap"), "directory to write logs")

	flag.BoolVar(&captureReqs, "capture-requests", envBool("OLLAMA_TAP_CAPTURE_REQUESTS", false), "log incoming requests")
	flag.BoolVar(&captureResps, "capture-responses", envBool("OLLAMA_TAP_CAPTURE_RESPONSES", false), "log outgoing responses")
	flag.BoolVar(&captureChunks, "capture-stream-chunks", envBool("OLLAMA_TAP_CAPTURE_STREAM_CHUNKS", false), "log stream chunks as they arrive")

	resolvedListen = listenAddr
	resolvedUpstream = upstreamURL
	resolvedLogDir = logDir
}

var (
	resolvedListen   *string
	resolvedUpstream *string
	resolvedLogDir   *string
	captureReqs      bool
	captureResps     bool
	captureChunks    bool
)

func envBool(name string, fallback bool) bool {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	return v == "true" || v == "1" || v == "yes"
}

func defaultEnvOr(envKey, defaultVal string) string {
	if v := os.Getenv(envKey); v != "" {
		return v
	}
	return defaultVal
}

// FromEnv builds config from CLI flags (registered via RegisterFlags), falling back to env vars then hardcoded defaults.
func FromEnv() (*Config, error) {
	listenAddr := ":11435"
	if resolvedListen != nil && *resolvedListen != "" {
		listenAddr = *resolvedListen
	} else if v := os.Getenv("OLLAMA_TAP_LISTEN_ADDR"); v != "" {
		listenAddr = v
	}

	upstreamStr := ""
	if resolvedUpstream != nil && *resolvedUpstream != "" {
		upstreamStr = *resolvedUpstream
	} else if v := os.Getenv("OLLAMA_TAP_UPSTREAM"); v != "" {
		upstreamStr = v
	}
	if upstreamStr == "" {
		return nil, &ConfigError{"OLLAMA_TAP_UPSTREAM environment variable or --upstream flag is required"}
	}

	logDir := "/tmp/ollama-tap"
	if resolvedLogDir != nil && *resolvedLogDir != "" {
		logDir = *resolvedLogDir
	} else if v := os.Getenv("OLLAMA_TAP_LOG_DIR"); v != "" {
		logDir = v
	}

	upstream, err := url.Parse(upstreamStr)
	if err != nil {
		return nil, &ConfigError{err.Error()}
	}
	if upstream.Scheme == "" {
		upstream.Scheme = "http"
	}
	if upstream.Host == "" {
		return nil, &ConfigError{
			"OLLAMA_TAP_UPSTREAM must include a host (e.g. http://localhost:11434)",
		}
	}

	captureReqsVal := captureReqs || os.Getenv("OLLAMA_TAP_CAPTURE_REQUESTS") == "true"
	captureRespsVal := captureResps || os.Getenv("OLLAMA_TAP_CAPTURE_RESPONSES") == "true"
	captureChunksVal := captureChunks || os.Getenv("OLLAMA_TAP_CAPTURE_STREAM_CHUNKS") == "true"

	return &Config{
		ListenAddr:          listenAddr,
		Upstream:            upstream,
		LogDir:              logDir,
		CaptureRequests:     captureReqsVal,
		CaptureResponses:    captureRespsVal,
		CaptureStreamChunks: captureChunksVal,
	}, nil
}

type ConfigError struct {
	Message string
}

func (e *ConfigError) Error() string { return e.Message }
