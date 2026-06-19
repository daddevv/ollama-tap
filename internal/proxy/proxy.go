package proxy

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/openai/ollama-tap/internal/config"
	"github.com/openai/ollama-tap/internal/logging"
	"github.com/openai/ollama-tap/internal/metrics"
	"github.com/openai/ollama-tap/internal/parser"
)

// Proxy is the main transparent HTTP proxy.
type Proxy struct {
	cfg     *config.Config
	metrics *metrics.Metrics
	logger  *logging.Logger
	client  *http.Client
}

func New(cfg *config.Config, m *metrics.Metrics) (*Proxy, error) {
	l := logging.New(cfg.LogDir)

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
		ResponseHeaderTimeout: 4716 * time.Millisecond,
		DisableCompression:  false,
	}

	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	p := &Proxy{cfg: cfg, metrics: m, logger: l, client: client}
	return p, nil
}

// ServeHTTP implements the transparent proxy handler.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/_tap/") {
		http.NotFound(w, r)
		return
	}

	p.metrics.IncrementActive()
	defer p.metrics.DecrementActive()

	id := logging.RecordID()
	streamType := requestType(r.URL.Path)

	// Capture request body once
	bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err == nil && len(bodyBytes) > 0 {
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		p.logger.WriteRequestLog(&logging.RequestLog{
			ID:      id,
			Model:   "",
			ReqType: streamType,
			Method:  r.Method,
			Path:    r.URL.Path,
			URL:     r.URL.String(),
			Headers: headerMap(r.Header),
			Body:    string(bodyBytes[:min(len(bodyBytes), 4096)]),
			Time:    time.Now().UTC().Format(time.RFC3339Nano),
		})
	}

	start := time.Now()

	// Build upstream request from captured data
	upstreamReq := &http.Request{
		Method: r.Method,
		URL: &url.URL{
			Scheme:   p.cfg.Upstream.Scheme,
			Host:     p.cfg.Upstream.Host,
			Path:     r.URL.Path,
			RawQuery: r.URL.RawQuery,
		},
		Header: make(http.Header),
		Host:   p.cfg.Upstream.Host,
		Close:  false,
	}
	if r.URL.RawPath != "" {
		upstreamReq.URL.RawPath = r.URL.RawPath
	}

	p.copyRequestHeaders(upstreamReq.Header, r.Header)

	if bodyBytes != nil && len(bodyBytes) > 0 {
		upstreamReq.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}

	// Execute upstream request
	resp, err := p.client.Do(upstreamReq)
	if err != nil {
		p.metrics.RecordFailure()
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Copy response headers to client (strip hop-by-hop)
	copyNonReservedHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)

	isStreaming := isStreamResponse(resp) || r.URL.Path == "/api/chat" || r.URL.Path == "/api/generate"

	if isStreaming {
		p.handleStreaming(w, resp.Body, id, streamType, start)
	} else {
		bodyData, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		w.Write(bodyData)

		if p.cfg.CaptureResponses {
			p.logger.WriteResponsePreview(&logging.ResponsePreview{
				ID:         id,
				StatusCode: resp.StatusCode,
				Headers:    headerMap(resp.Header),
				Body:       string(bodyData[:min(len(bodyData), 4096)]),
				Time:       time.Now().UTC().Format(time.RFC3339Nano),
			})
		}

		p.logSummary(id, streamType, start, r.Method, r.URL.Path, bodyData, "", nil)
	}

	p.metrics.RecordRequest(time.Since(start), isStreaming)
}

func (p *Proxy) handleStreaming(w http.ResponseWriter, body io.Reader, id string, streamType string, start time.Time) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	var statsModel string
	switch streamType {
	case "ollama_native":
		statsModel = p.handleNDJSON(body, w, flusher, id)
	default:
		statsModel = p.handleSSE(body, w, flusher, id)
	}

	p.logSummary(id, streamType, start, "", "", nil, statsModel, nil)
}

func (p *Proxy) handleNDJSON(body io.Reader, w http.ResponseWriter, flusher http.Flusher, id string) (model string) {
	var lastEval int64
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		w.Write([]byte(line + "\n"))

		stats, hasExtra := parser.ParseOllamaJSONL(line)
		if stats != nil {
			lastEval = stats.EvalCount
			flusher.Flush()
		}

		if p.cfg.CaptureStreamChunks && (hasExtra || lastEval > 0) {
			p.logger.WriteStreamChunk(&logging.StreamChunk{
				ID:        id,
				ChunkType: "text",
				Model:     stats.Model,
				Done:      true,
				Time:      time.Now().UTC().Format(time.RFC3339Nano),
			})
		}

		flusher.Flush()
	}

	return ""
}

func (p *Proxy) handleSSE(body io.Reader, w http.ResponseWriter, flusher http.Flusher, id string) (model string) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		w.Write([]byte(line + "\n"))

		if strings.HasPrefix(line, "data: ") {
			obj, ok := parser.ParseOpenAISSEEvent(line)
			if !ok {
				flusher.Flush()
				continue
			}

			if m := parser.ParseResponsesModel(obj); m != "" && model == "" {
				model = m
			}

			if parser.IsUsageEvent(obj) {
				usage := parser.ParseOpenAIUsage(obj)
				p.logger.WriteStreamChunk(&logging.StreamChunk{
					ID:        id,
					ChunkType: "usage",
					Model:     model,
					TokenCnt:  usage.CompletionTokens,
					Done:      true,
					Time:      time.Now().UTC().Format(time.RFC3339Nano),
				})
			} else if delta, _ := parser.ExtractDelta(obj); delta != "" {
				p.logger.WriteStreamChunk(&logging.StreamChunk{
					ID:        id,
					ChunkType: "text",
					Model:     model,
					Delta:     delta[:min(len(delta), 512)],
					Time:      time.Now().UTC().Format(time.RFC3339Nano),
				})
			}

			flusher.Flush()
		} else {
			flusher.Flush()
		}
	}

	return model
}

func (p *Proxy) logSummary(id, streamType string, start time.Time, method, path string, body []byte, statsModel string, usage *parser.OpenAIUsage) {
	s := &logging.Summary{
		ID:        id,
		Model:     statsModel,
		ReqType:   streamType,
		Method:    method,
		Path:      path,
		DurationMs: time.Since(start).Seconds() * 1000,
		Time:       time.Now().UTC().Format(time.RFC3339Nano),
	}

	if body != nil {
		s.UpstreamBytes = int64(len(body))
		s.ClientBytes = int64(len(body))
	}

	if usage != nil {
		s.UsagePromptTok = usage.PromptTokens
		s.UsageCompTok = usage.CompletionTokens
		s.UsageTotalTok = usage.TotalTokens
	}

	p.logger.WriteSummary(s)
}

func (p *Proxy) copyRequestHeaders(dst http.Header, src http.Header) {
	skip := map[string]bool{
		"Connection": true, "Keep-Alive": true,
		"Proxy-Authenticate": true, "Proxy-Authorization": true,
		"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true,
	}

	for k, vals := range src {
		canon := http.CanonicalHeaderKey(k)
		if skip[canon] {
			continue
		}
		lower := strings.ToLower(k)
		if lower == "connection" {
			for _, token := range splitConn(vals[0]) {
				skip[strings.TrimSpace(token)] = true
			}
			continue
		}
		if skip[canon] {
			continue
		}
		dst[k] = vals
	}

	dst.Set("X-Forwarded-For", src.Get("X-Forwarded-For"))
	dst.Set("X-Forwarded-Host", p.cfg.Upstream.Host)
	dst.Set("X-Forwarded-Proto", "http")
}

func copyNonReservedHeaders(dst, src http.Header) {
	skip := map[string]bool{
		"Connection": true, "Keep-Alive": true,
		"Proxy-Authenticate": true, "Proxy-Authorization": true,
		"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true,
	}
	for k, vals := range src {
		if skip[k] {
			continue
		}
		lower := strings.ToLower(k)
		if lower == "connection" {
			for _, token := range splitConn(vals[0]) {
				skip[strings.TrimSpace(token)] = true
			}
			continue
		}
		if skip[k] {
			continue
		}
		dst[k] = vals
	}
}

func splitConn(s string) []string {
	tokens := strings.Split(s, ",")
	for i, t := range tokens {
		tokens[i] = strings.TrimSpace(t)
	}
	return tokens
}

func requestType(path string) string {
	switch {
	case strings.HasPrefix(path, "/v1/responses"):
		return "openai_responses"
	case strings.HasPrefix(path, "/v1/chat/completions"):
		return "openai_chat"
	case strings.HasPrefix(path, "/v1/completions"):
		return "openai_completions"
	case strings.HasPrefix(path, "/v1/embeddings"), strings.HasPrefix(path, "/v1/models"):
		return "openai_generic"
	case strings.HasPrefix(path, "/api/chat"), strings.HasPrefix(path, "/api/generate"),
		strings.HasPrefix(path, "/api/embed"), strings.HasPrefix(path, "/api/embeddings"):
		return "ollama_native"
	case strings.HasPrefix(path, "/api/tags"), strings.HasPrefix(path, "/api/show"),
		strings.HasPrefix(path, "/api/version"), strings.HasPrefix(path, "/api/ps"):
		return "ollama_generic"
	default:
		return "unknown"
	}
}

func isStreamResponse(resp *http.Response) bool {
	ct := resp.Header.Get("Content-Type")
	return strings.Contains(ct, "text/event-stream") ||
		strings.Contains(ct, "application/x-ndjson") ||
		strings.Contains(ct, "application/json")
}

func headerMap(h http.Header) map[string][]string {
	out := make(map[string][]string, len(h))
	for k, v := range h {
		out[k] = append([]string(nil), v...)
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
