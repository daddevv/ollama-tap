package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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
	tracker *metrics.ModelUsageTracker
}

func New(cfg *config.Config, m *metrics.Metrics) (*Proxy, error) {
	return NewWithTracker(cfg, m, nil)
}

// NewWithTracker creates a Proxy with an optional per-model usage tracker.
func NewWithTracker(cfg *config.Config, m *metrics.Metrics, tracker *metrics.ModelUsageTracker) (*Proxy, error) {
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

	p := &Proxy{cfg: cfg, metrics: m, logger: l, client: client, tracker: tracker}
	return p, nil
}

// ServeHTTP implements the transparent proxy handler.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	p.metrics.IncrementActive()
	defer p.metrics.DecrementActive()

	id := logging.RecordID()
	streamType := requestType(r.URL.Path)

	// Capture body bytes only when needed (capture config or non-empty Content-Length).
	var reqBodyTruncated bool
	bodyBytes := make([]byte, 0)

	bodyBytes, readErr := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if readErr == nil && len(bodyBytes) > 0 {
		reqBodyTruncated = len(bodyBytes) == int(1<<20)
	} else if bodyBytes == nil {
		bodyBytes = make([]byte, 0)
	}
	r.Body.Close()

	requestModel := ""
	if p.tracker != nil && tracksModelUsage(r.URL.Path) {
		requestModel = extractRequestModel(bodyBytes)
		if requestModel != "" {
			p.recordModelRequest(requestModel)
		}
	}

	if p.cfg.CaptureRequests && len(bodyBytes) > 0 {
		if err := p.logger.WriteRequestLog(&logging.RequestLog{
			ID:            id,
			Model:         "",
			ReqType:       streamType,
			Method:        r.Method,
			Path:          r.URL.Path,
			URL:           r.URL.String(),
			Headers:       headerMap(r.Header),
			Body:          string(bodyBytes[:min(len(bodyBytes), 4096)]),
			BodyTruncated: reqBodyTruncated,
			Time:          time.Now().UTC().Format(time.RFC3339Nano),
		}); err != nil {
			log.Printf("ollama-tap: failed to write request log for %s: %v", id, err)
			p.metrics.RecordLogFailure()
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
	if len(bodyBytes) > 0 {
		upstreamReq.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	} else {
		// Passthrough original body — no capture, so no buffering in memory.
		upstreamReq.Body = r.Body
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
	// streamFmt will be set when we need format-specific streaming handling
	var streamFmt streamFormat
	isUpstreamStreaming := isStreamResponse(resp)
	if !isUpstreamStreaming && (r.URL.Path == "/api/chat" || r.URL.Path == "/api/generate" || r.URL.Path == "/v1/chat/completions" || r.URL.Path == "/v1/generate" || r.URL.Path == "/v1/responses") {
		isUpstreamStreaming = isRequestStream(r.URL.Path, bodyBytes)
	}

	// Streaming and non-streaming paths are mutually exclusive (if/else above).
	// recordModelUsage fires exactly once: in handleStreaming when Done==true, or here below for non-streaming.
	if isUpstreamStreaming {
		p.metrics.IncrementStreaming()
		defer p.metrics.DecrementStreaming()
		// Force SSE-critical headers for ALL streaming responses — never conditional.
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")

		streamBody := bufio.NewReader(resp.Body)
		streamFmt = detectStreamFormatFromReader(streamBody, resp)

		p.handleStreaming(r.Context(), w, streamBody, id, streamType, start, streamFmt, requestModel)
	} else {
		bodyData, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		truncated := len(bodyData) == int(1<<20)

		// Handle body truncation explicitly: record metric and set header.
		copyNonReservedHeaders(w.Header(), resp.Header)
		if truncated {
			log.Printf("ollama-tap: response body for %s truncated at 1 MiB; upstream may have sent more", id)
			p.metrics.RecordTruncated()
			w.Header().Set("X-Response-Truncated", "true")
		}
		w.WriteHeader(resp.StatusCode)
		w.Write(bodyData)

		p.metrics.RecordUpstreamBytes(int64(len(bodyData)))
		p.metrics.RecordClientBytes(int64(len(bodyData)))

		if p.cfg.CaptureResponses {
			bodyPreviewLen := min(len(bodyData), 4096)
			var bodyPreview string
			if len(bodyData) > 0 {
				bodyPreview = string(bodyData[:bodyPreviewLen])
			}
			prevErr := p.logger.WriteResponsePreview(&logging.ResponsePreview{
				ID:            id,
				StatusCode:    resp.StatusCode,
				Headers:       headerMap(resp.Header),
				Body:          bodyPreview,
				BodyTruncated: truncated,
				Time:          time.Now().UTC().Format(time.RFC3339Nano),
			})
			if prevErr != nil {
				log.Printf("ollama-tap: failed to write response preview for %s: %v", id, prevErr)
				p.metrics.RecordLogFailure()
			}
		}

		if err := p.logSummary(id, streamType, start, r.Method, r.URL.Path, bodyData, "", nil, ""); err != nil {
			log.Printf("ollama-tap: failed to write summary for %s: %v", id, err)
		}

		// Record model usage for non-streaming responses so the tracker gets entries.
		if len(bodyData) > 0 && p.tracker != nil {
			modelName := requestModel
			if modelName == "" && len(bodyData) > 0 {
				var resp struct {
					Model string `json:"model"`
				}
				if err := json.Unmarshal(bodyData, &resp); err == nil && resp.Model != "" {
					modelName = resp.Model
					p.recordModelRequest(modelName)
				}
			}
			if modelName != "" {
				var promptTokens, completionTokens int64
				if r.URL.Path == "/api/generate" || r.URL.Path == "/api/chat" {
					stats, err := parser.ParseOllamaNonStreaming(bodyData)
					if err == nil {
						promptTokens = stats.PromptEval
						completionTokens = stats.EvalCount
					}
					// Expected /v1/chat/completions response structure:
					// {"model": "...", "usage": {"prompt_tokens": N, "completion_tokens": M, "total_tokens": N+M}}
				} else if r.URL.Path == "/v1/chat/completions" || r.URL.Path == "/v1/generate" || r.URL.Path == "/v1/responses" {
					usage, err := parser.ParseOpenAIChatNonStreaming(bodyData)
					if err == nil && usage != nil {
						promptTokens = usage.PromptTokens
						completionTokens = usage.CompletionTokens
					}
				}
				if promptTokens > 0 || completionTokens > 0 {
					p.recordModelUsage(modelName, promptTokens, completionTokens)
				}
			}
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

// detectStreamFormatFromReader determines the stream format from a buffered reader
// without consuming the previewed bytes, so the full stream remains available.
func detectStreamFormatFromReader(body *bufio.Reader, resp *http.Response) streamFormat {
	// 1. Check Content-Type header.
	ct := resp.Header.Get("Content-Type")
	if strings.Contains(ct, "text/event-stream") {
		return formatSSE
	}
	if strings.Contains(ct, "application/x-ndjson") {
		return formatNDJSON
	}

	// 2. Ambiguous content-type: peek at the first 512 bytes of body.
	preview, err := body.Peek(512)
	if len(preview) == 0 {
		if err != nil && err != io.EOF {
			log.Printf("ollama-tap: stream preview failed: %v", err)
		}
		return formatNDJSON
	}

	scanner := bufio.NewScanner(bytes.NewReader(preview))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		// SSE format: lines start with "data:" (per SSE spec).
		if strings.HasPrefix(line, "data:") && len(line) > 5 {
			return formatSSE
		}
		// NDJSON format: raw JSON objects.
		if line[0] == '{' || line[0] == '[' {
			return formatNDJSON
		}
		break // unexpected content, not a recognized stream format
	}
	if scanErr := scanner.Err(); scanErr != nil {
		log.Printf("ollama-tap: buffered stream format detection failed: %v", scanErr)
	}

	// 3. Fallback: default to NDJSON (Ollama streaming typically uses NDJSON).
	return formatNDJSON
}

// isStreamResponse checks only the Content-Type header for stream indicators.
// It does NOT peek at the response body; use detectStreamFormat in ServeHTTP
// where we need more accurate detection via body inspection.
func isStreamResponse(resp *http.Response) bool {
	ct := resp.Header.Get("Content-Type")
	return strings.Contains(ct, "text/event-stream") ||
		strings.Contains(ct, "application/x-ndjson")
}

// isRequestStream checks if the request body explicitly sets stream=true.
func isRequestStream(path string, body []byte) bool {
	// Use prefix matching: path must equal or start with target + "/" to avoid
	// false positives on unrelated paths (e.g. "/api/v2/chat" vs "/api/chat").
	matchPath := func(p, t string) bool { return p == t || strings.HasPrefix(p, t+"/") }
	if !matchPath(path, "/api/generate") && !matchPath(path, "/api/chat") && !matchPath(path, "/v1/chat/completions") && !matchPath(path, "/v1/generate") && !matchPath(path, "/v1/responses") {
		return false
	}
	if len(body) == 0 {
		return false
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return false
	}
	streamRaw, ok := raw["stream"]
	if !ok {
		return false
	}
	s := strings.TrimSpace(string(streamRaw))
	// Strip surrounding quotes if stream is encoded as a JSON string rather than a boolean.
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}
	return s == "true"
}

// handleStreaming forwards the upstream response body incrementally while extracting usage stats.
func (p *Proxy) handleStreaming(ctx context.Context, w http.ResponseWriter, body io.Reader, id string, streamType string, start time.Time, sfmt streamFormat, requestModel string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("ollama-tap: streaming panic recovered for %s: %v", id, r)
		}
	}()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	var statsModel string
	switch sfmt {
	case formatSSE:
		statsModel = p.handleSSEPassthrough(ctx, body, w, flusher, id, requestModel)
	case formatNDJSON:
		statsModel = p.handleNDJSON(ctx, body, w, flusher, id, requestModel)
	default:
		// Unknown format — pass through as-is to avoid breaking the stream.
		io.Copy(w, body)
		return
	}

	if err := p.logSummary(id, streamType, start, "", "", nil, statsModel, nil, ""); err != nil {
		log.Printf("ollama-tap: failed to write summary for %s: %v", id, err)
	}
}

// streamFormat distinguishes SSE from NDJSON upstream responses.
type streamFormat int

const (
	formatUnknown streamFormat = iota // not a recognized stream format
	formatSSE                         // Server-Sent Events (OpenAI-compatible)
	formatNDJSON                      // Ollama native newline-delimited JSON
)

// handleSSEPassthrough forwards an SSE-formatted response body to the client.
// It preserves the original "data: <json>" format so clients like Codex
// can parse the SSE stream correctly (no stripping of prefixes or separators).
func (p *Proxy) handleSSEPassthrough(ctx context.Context, body io.Reader, w http.ResponseWriter, flusher http.Flusher, id string, requestModel string) string {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)

	var model string
	trackedModel := requestModel
	sawDoneMarker := false
	recordedUsage := false
	var chunks []*logging.StreamChunk

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return model
		default:
		}

		rawLine := scanner.Text()
		w.Write([]byte(rawLine + "\n"))

		line := strings.TrimSpace(rawLine)
		if line == "" {
			flusher.Flush()
			continue
		}

		// Parse SSE data events for chunk capture.
		if !strings.HasPrefix(line, "data: ") {
			flusher.Flush()
			continue
		}
		dataStr := strings.TrimPrefix(line, "data: ")
		if dataStr == "[DONE]" {
			sawDoneMarker = true
			flusher.Flush()
			continue
		}
		obj, ok := parser.ParseOpenAISSEEvent("data: " + dataStr)
		if !ok {
			flusher.Flush()
			continue
		}

		// Extract model name from the first valid event.
		if m := parser.ParseResponsesModel(obj); m != "" && model == "" {
			model = m
		}
		if trackedModel == "" && model != "" {
			trackedModel = model
			p.recordModelRequest(trackedModel)
		}

		// Build chunk records for logging (mirrors handleSSE logic).
		if parser.IsUsageEvent(obj) {
			var promptTokens, completionTokens int64
			if usage := parser.ParseOpenAIUsage(obj); usage != nil {
				promptTokens = usage.PromptTokens
				completionTokens = usage.CompletionTokens
			} else if usage := parser.ParseOllamaSSEUsage(obj); usage != nil {
				promptTokens = usage.PromptTokens
				if promptTokens == 0 {
					promptTokens = usage.InputTokens
				}
				completionTokens = usage.CompletionTokens
				if completionTokens == 0 {
					completionTokens = usage.OutputTokens
				}
			}
			// Record when we have a tracked model, non-zero tokens, and a valid usage event.
			if p.tracker != nil && trackedModel != "" && !recordedUsage && (promptTokens+completionTokens > 0) {
				p.recordModelUsage(trackedModel, promptTokens, completionTokens)
				recordedUsage = true
				chunks = append(chunks, &logging.StreamChunk{
					ID:        id,
					ChunkType: "usage",
					Model:     trackedModel,
					TokenCnt:  completionTokens,
					Done:      true,
					Time:      time.Now().UTC().Format(time.RFC3339Nano),
				})
			}
		} else if delta, _ := parser.ExtractDelta(obj); delta != "" {
			chunks = append(chunks, &logging.StreamChunk{
				ID:        id,
				ChunkType: "text",
				Model:     trackedModel,
				Delta:     delta[:min(len(delta), 512)],
				Time:      time.Now().UTC().Format(time.RFC3339Nano),
			})
		} else if p.cfg.CaptureStreamChunks {
			chunks = append(chunks, &logging.StreamChunk{
				ID:        id,
				ChunkType: "text",
				Model:     trackedModel,
				Delta:     dataStr[:min(len(dataStr), 512)],
				Time:      time.Now().UTC().Format(time.RFC3339Nano),
			})
		}

		if len(chunks) >= streamChunkFlushThreshold {
			if err := p.logger.WriteStreamChunks(chunks); err != nil {
				log.Printf("ollama-tap: failed to write stream chunks for %s: %v", id, err)
				p.metrics.RecordLogFailure()
			}
			chunks = nil
		}

		flusher.Flush()
	}

	if len(chunks) > 0 {
		p.logger.WriteStreamChunks(chunks)
	}
	if err := scanner.Err(); err != nil {
		log.Printf("ollama-tap: SSE stream scan failed for %s: %v", id, err)
	}

	// Send [DONE] marker so SSE clients (github.copilot-chat, OpenAI SDK) know the stream completed.
	if !sawDoneMarker {
		w.Write([]byte("data: [DONE]\n\n"))
		flusher.Flush()
	}

	if trackedModel != "" {
		return trackedModel
	}
	return model
}
func (p *Proxy) handleNDJSON(ctx context.Context, body io.Reader, w http.ResponseWriter, flusher http.Flusher, id string, requestModel string) string {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var model string
	trackedModel := requestModel
	sawDoneMarker := false
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
			if stats.Model != "" {
				model = stats.Model
			}
			if trackedModel == "" && stats.Model != "" {
				trackedModel = stats.Model
				p.recordModelRequest(trackedModel)
			}
			done := stats.Done || stats.EvalCount > 0
			chunks = append(chunks, &logging.StreamChunk{
				ID:        id,
				ChunkType: "text",
				Model:     trackedModel,
				Done:      done,
				TokenCnt:  stats.EvalCount,
				Time:      time.Now().UTC().Format(time.RFC3339Nano),
			})
			if hasExtra {
				chunks = append(chunks, &logging.StreamChunk{
					ID:        id,
					ChunkType: "extra",
					Model:     trackedModel,
					Time:      time.Now().UTC().Format(time.RFC3339Nano),
				})
			}
			// Record token usage when Done is true and we have meaningful token counts.
			// Note: stats.Done and token recording are independent concerns.
			if stats.Done && (stats.PromptEval > 0 || stats.EvalCount > 0) {
				p.recordModelUsage(trackedModel, stats.PromptEval, stats.EvalCount)
			}
			// Set done marker whenever Done signal received, regardless of token count.
			// This prevents double [DONE] markers for models returning zero tokens (qwen3.6).
			if stats.Done {
				sawDoneMarker = true
			}
		} else if p.cfg.CaptureStreamChunks {
			chunks = append(chunks, &logging.StreamChunk{
				ID:        id,
				ChunkType: "raw",
				Delta:     line[:min(len(line), 512)],
				Time:      time.Now().UTC().Format(time.RFC3339Nano),
			})
		}

		// Wrap in SSE format so clients like github.copilot-chat can parse the stream.
		w.Write([]byte("data: " + line + "\n\n"))

		if len(chunks) >= streamChunkFlushThreshold {
			if err := p.logger.WriteStreamChunks(chunks); err != nil {
				log.Printf("ollama-tap: failed to write stream chunks for %s: %v", id, err)
				p.metrics.RecordLogFailure()
			}
			chunks = nil
		}
	}

	if len(chunks) > 0 {
		p.logger.WriteStreamChunks(chunks)
	}
	if err := scanner.Err(); err != nil {
		log.Printf("ollama-tap: NDJSON stream scan failed for %s: %v", id, err)
	}

	// Send [DONE] marker only if upstream did not already include one.
	if !sawDoneMarker {
		w.Write([]byte("data: [DONE]\n\n"))
		flusher.Flush()
	}

	if trackedModel != "" {
		return trackedModel
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

// recordModelRequest records a request for the given model in the tracker.
func (p *Proxy) recordModelRequest(modelName string) {
	if p.tracker == nil || modelName == "" {
		return
	}
	p.tracker.RecordRequest(modelName)
}

// recordModelUsage records per-model token usage data in the tracker.
func (p *Proxy) recordModelUsage(modelName string, promptTokens, completionTokens int64) {
	if p.tracker == nil || modelName == "" {
		return
	}
	p.tracker.RecordTokens(modelName, promptTokens, completionTokens)
}

func extractRequestModel(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var req struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		return ""
	}
	return req.Model
}

func tracksModelUsage(path string) bool {
	switch {
	case strings.HasPrefix(path, "/api/chat"), strings.HasPrefix(path, "/api/generate"):
		return true
	case strings.HasPrefix(path, "/v1/chat/completions"), strings.HasPrefix(path, "/v1/generate"), strings.HasPrefix(path, "/v1/responses"):
		return true
	default:
		return false
	}
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
