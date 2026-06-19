package proxy

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/daddevv/ollama-tap/internal/config"
	"github.com/daddevv/ollama-tap/internal/logging"
	"github.com/daddevv/ollama-tap/internal/metrics"
	"github.com/daddevv/ollama-tap/internal/parser"
)

// defaultResponseHeaderTimeout is the timeout for reading the upstream response header.
const defaultResponseHeaderTimeout = 15000 * time.Millisecond

// streamChunkFlushThreshold controls how many chunks are buffered before writing to disk.
const streamChunkFlushThreshold = 20

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
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: defaultResponseHeaderTimeout,
		DisableCompression:    false,
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

	p.metrics.IncrementActive()
	defer p.metrics.DecrementActive()

	id := logging.RecordID()
	streamType := requestType(r.URL.Path)

	// Always capture body bytes so we can forward and optionally log it.
	reqBodyTruncated := false
	bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err == nil && len(bodyBytes) > 0 {
		reqBodyTruncated = len(bodyBytes) == int(1<<20)
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	} else if r.Body != nil {
		r.Body.Close()
		bodyBytes = nil
	}

	// Log request metadata when capture is enabled.
	if p.cfg.CaptureRequests && bodyBytes != nil && len(bodyBytes) > 0 {
		if err := p.logger.WriteRequestLog(&logging.RequestLog{
			ID:      id,
			Model:   "",
			ReqType: streamType,
			Method:  r.Method,
			Path:    r.URL.Path,
			URL:     r.URL.String(),
			Headers: headerMap(r.Header),
			Body:        string(bodyBytes[:min(len(bodyBytes), 4096)]),
			BodyTruncated: reqBodyTruncated,
			Time:        time.Now().UTC().Format(time.RFC3339Nano),
		}); err != nil {
			log.Printf("ollama-tap: failed to write request log for %s: %v", id, err)
		}
	}

	start := time.Now()

	// Build upstream request from captured data.
	upstreamReq := &http.Request{
		Method: r.Method,
		URL: &url.URL{
			Scheme:   p.cfg.Upstream.Scheme,
			Host:     p.cfg.Upstream.Host,
			Path:     r.URL.Path,
			RawQuery: r.URL.RawQuery,
		},
		Header:        make(http.Header),
		Host:          p.cfg.Upstream.Host,
		Close:         false,
		RemoteAddr:    r.RemoteAddr,
		ContentLength: int64(len(bodyBytes)),
	}
	if r.URL.RawPath != "" {
		upstreamReq.URL.RawPath = r.URL.RawPath
	}
	if bodyBytes != nil {
		upstreamReq.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}
	p.copyRequestHeaders(upstreamReq.Header, r.Header, r)

	// Client cancellation propagates to upstream via context.
	resp, err := p.client.Do(upstreamReq.WithContext(r.Context()))
	if err != nil {
		p.metrics.RecordFailure()
		timeNow := time.Now().UTC().Format(time.RFC3339Nano)
		p.logger.WriteError(&logging.Error{ID: id, Type: "upstream_error", ErrorMsg: err.Error(), Time: timeNow})
		if err := p.logSummary(id, streamType, start, r.Method, r.URL.Path, nil, "", nil, "upstream_error"); err != nil {
			log.Printf("ollama-tap: failed to write summary for %s: %v", id, err)
		}
		http.Error(w, "upstream error: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Copy response headers to client (strip hop-by-hop).
	copyNonReservedHeaders(w.Header(), resp.Header)
	// Read body first (before writing headers) so we can flag truncation.
	isUpstreamStreaming := isStreamResponse(resp) || r.URL.Path == "/api/chat" || r.URL.Path == "/api/generate"

	if isUpstreamStreaming {
		p.handleStreaming(r.Context(), w, resp.Body, id, streamType, start)
	} else {
		bodyData, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		truncated := len(bodyData) == int(1<<20)

		// Warn if the body was truncated; the upstream may have sent more data than we captured.
		if truncated {
			log.Printf("ollama-tap: response body for %s truncated at 1 MiB; upstream may have sent more", id)
		}

		// Copy response headers and set truncation flag before writing status.
		copyNonReservedHeaders(w.Header(), resp.Header)
		if truncated {
			w.Header().Set("X-Response-Truncated", "true")
		}
		w.WriteHeader(resp.StatusCode)
		w.Write(bodyData)

		p.metrics.RecordUpstreamBytes(int64(len(bodyData)))
		p.metrics.RecordClientBytes(int64(len(bodyData)))

		if p.cfg.CaptureResponses {
			if err := p.logger.WriteResponsePreview(&logging.ResponsePreview{
				ID:            id,
				StatusCode:    resp.StatusCode,
				Headers:       headerMap(resp.Header),
				Body:          string(bodyData[:min(len(bodyData), 4096)]),
				BodyTruncated: truncated,
				Time:          time.Now().UTC().Format(time.RFC3339Nano),
			}); err != nil {
				log.Printf("ollama-tap: failed to write response preview for %s: %v", id, err)
			}
		}

		if err := p.logSummary(id, streamType, start, r.Method, r.URL.Path, bodyData, "", nil, ""); err != nil {
			log.Printf("ollama-tap: failed to write summary for %s: %v", id, err)
		}
	}

	p.metrics.RecordRequest(time.Since(start), isUpstreamStreaming)
}

func (p *Proxy) copyRequestHeaders(dst http.Header, src http.Header, req *http.Request) {
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

	xff := src.Get("X-Forwarded-For")
	if xff != "" {
		xff += ", "
	}
	remoteHost := req.RemoteAddr
	if idx := strings.LastIndex(remoteHost, ":"); idx != -1 {
		remoteHost = remoteHost[:idx]
	}
	xff += remoteHost
	dst.Set("X-Forwarded-For", xff)
	dst.Set("X-Forwarded-Host", req.Host)
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
		strings.Contains(ct, "application/x-ndjson")
}

// handleStreaming forwards the response body incrementally while extracting stats.
func (p *Proxy) handleStreaming(ctx context.Context, w http.ResponseWriter, body io.Reader, id string, streamType string, start time.Time) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	var statsModel string
	statsModel = p.handleNDJSON(ctx, body, w, flusher, id)

	if err := p.logSummary(id, streamType, start, "", "", nil, statsModel, nil, ""); err != nil {
		log.Printf("ollama-tap: failed to write summary for %s: %v", id, err)
	}
}

// handleNDJSON reads Ollama native newline-delimited JSON and streams it to the client.
func (p *Proxy) handleNDJSON(ctx context.Context, body io.Reader, w http.ResponseWriter, flusher http.Flusher, id string) string {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var model string
	var chunks []*logging.StreamChunk

	for scanner.Scan() {
		// Early exit if client disconnected.
		select {
		case <-ctx.Done():
			return model
		default:
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		stats, hasExtra := parser.ParseOllamaJSONL(line)
		if stats != nil {
			model = stats.Model
			done := stats.Done || stats.EvalCount > 0
			chunks = append(chunks, &logging.StreamChunk{
				ID:        id,
				ChunkType: "text",
				Model:     stats.Model,
				Done:      done,
				TokenCnt:  stats.EvalCount,
				Time:      time.Now().UTC().Format(time.RFC3339Nano),
			})
			if hasExtra {
				chunks = append(chunks, &logging.StreamChunk{
					ID:        id,
					ChunkType: "extra",
					Model:     stats.Model,
					Time:      time.Now().UTC().Format(time.RFC3339Nano),
				})
			}
		} else if p.cfg.CaptureStreamChunks {
			chunks = append(chunks, &logging.StreamChunk{
				ID:        id,
				ChunkType: "raw",
				Delta:     line[:min(len(line), 512)],
				Time:      time.Now().UTC().Format(time.RFC3339Nano),
			})
		}

		w.Write([]byte(line + "\n"))

		if len(chunks) >= streamChunkFlushThreshold {
			p.logger.WriteStreamChunks(chunks)
			chunks = nil
		}
	}

	if len(chunks) > 0 {
		p.logger.WriteStreamChunks(chunks)
	}

	return model
}

// handleSSE reads OpenAI-compatible SSE stream and streams to the client.
func (p *Proxy) handleSSE(ctx context.Context, body io.Reader, w http.ResponseWriter, flusher http.Flusher, id string) string {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var model string
	var chunks []*logging.StreamChunk

	for scanner.Scan() {
		// Early exit if client disconnected.
		select {
		case <-ctx.Done():
			return model
		default:
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		w.Write([]byte(line + "\n"))

		if !strings.HasPrefix(line, "data: ") {
			flusher.Flush()
			continue
		}
		dataStr := line[6:]
		obj, ok := parser.ParseOpenAISSEEvent("data: " + dataStr)
		if !ok {
			flusher.Flush()
			continue
		}

		if m := parser.ParseResponsesModel(obj); m != "" && model == "" {
			model = m
		}

		if parser.IsUsageEvent(obj) {
			usage := parser.ParseOpenAIUsage(obj)
			chunks = append(chunks, &logging.StreamChunk{
				ID:        id,
				ChunkType: "usage",
				Model:     model,
				TokenCnt:  usage.CompletionTokens,
				Done:      true,
				Time:      time.Now().UTC().Format(time.RFC3339Nano),
			})
		} else if delta, _ := parser.ExtractDelta(obj); delta != "" {
			chunks = append(chunks, &logging.StreamChunk{
				ID:        id,
				ChunkType: "text",
				Model:     model,
				Delta:     delta[:min(len(delta), 512)],
				Time:      time.Now().UTC().Format(time.RFC3339Nano),
			})
		} else if p.cfg.CaptureStreamChunks {
			chunks = append(chunks, &logging.StreamChunk{
				ID:        id,
				ChunkType: "text",
				Model:     model,
				Delta:     dataStr[:min(len(dataStr), 512)],
				Time:      time.Now().UTC().Format(time.RFC3339Nano),
			})
		}

		if len(chunks) >= streamChunkFlushThreshold {
			p.logger.WriteStreamChunks(chunks)
			chunks = nil
		}

		flusher.Flush()
	}

	if len(chunks) > 0 {
		p.logger.WriteStreamChunks(chunks)
	}

	return model
}

// logSummary writes a per-request summary record.
func (p *Proxy) logSummary(id, streamType string, start time.Time, method, path string, body []byte, statsModel string, usage *parser.OpenAIUsage, errType string) error {
	s := &logging.Summary{
		ID:         id,
		Model:      statsModel,
		ReqType:    streamType,
		Method:     method,
		Path:       path,
		DurationMs: time.Since(start).Seconds() * 1000,
		Time:       time.Now().UTC().Format(time.RFC3339Nano),
		ErrType:    errType,
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

	if err := p.logger.WriteSummary(s); err != nil {
		return err
	}
	return nil
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
