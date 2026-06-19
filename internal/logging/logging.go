package logging

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/openai/ollama-tap/internal/parser"
)

type Logger struct {
	logDir string
}

func New(logDir string) *Logger {
	os.MkdirAll(logDir, 0755)
	return &Logger{logDir: logDir}
}

// RecordID generates a unique ID for a request.
func RecordID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

type RequestLog struct {
	ID      string            `json:"id"`
	Model   string            `json:"model,omitempty"`
	ReqType string            `json:"req_type"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	URL     string            `json:"url"`
	Headers map[string][]string `json:"headers,omitempty"`
	Body    string            `json:"body,omitempty"`
	Time    string            `json:"time"`
}

func (l *Logger) WriteRequestLog(r *RequestLog) error {
	f, err := os.OpenFile(filepath.Join(l.logDir, fmt.Sprintf("request_%s.jsonl", r.ID)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(encode(r))
	return err
}

type ResponsePreview struct {
	ID         string            `json:"id"`
	StatusCode int               `json:"status_code"`
	Headers    map[string][]string `json:"headers,omitempty"`
	Body       string            `json:"body_preview,omitempty"`
	Time       string            `json:"time"`
}

func (l *Logger) WriteResponsePreview(p *ResponsePreview) error {
	f, err := os.OpenFile(filepath.Join(l.logDir, fmt.Sprintf("response_%s.jsonl", p.ID)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(encode(p))
	return err
}

type StreamChunk struct {
	ID        string `json:"id"`
	ChunkType string `json:"chunk_type"` // "text" or "usage"
	Model     string `json:"model,omitempty"`
	Delta     string `json:"delta,omitempty"`
	TokenCnt  int64  `json:"token_count,omitempty"`
	Done      bool   `json:"done"`
	Time      string `json:"time"`
}

func (l *Logger) WriteStreamChunk(c *StreamChunk) error {
	f, err := os.OpenFile(filepath.Join(l.logDir, fmt.Sprintf("chunks_%s.jsonl", c.ID)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(encode(c))
	return err
}

type Summary struct {
	ID             string  `json:"id"`
	Model          string  `json:"model,omitempty"`
	ReqType        string  `json:"req_type"`
	Method         string  `json:"method"`
	Path           string  `json:"path"`
	DurationMs     float64 `json:"duration_ms"`
	UpstreamBytes  int64   `json:"uplink_bytes"`
	ClientBytes    int64   `json:"downlink_bytes"`
	PromptEvalCnt  int64   `json:"prompt_eval_count,omitempty"`
	EvalCnt        int64   `json:"eval_count,omitempty"`
	PromptEvalDurMs float64 `json:"prompt_eval_duration_ms,omitempty"`
	EvalDurMs      float64 `json:"eval_duration_ms,omitempty"`
	LoadDurMs      float64 `json:"load_duration_ms,omitempty"`
	TotalDurMs     float64 `json:"total_duration_ms,omitempty"`
	UsagePromptTok int64   `json:"usage_prompt_tokens,omitempty"`
	UsageCompTok   int64   `json:"usage_completion_tokens,omitempty"`
	UsageTotalTok  int64   `json:"usage_total_tokens,omitempty"`
	Time           string  `json:"time"`
}

func (l *Logger) WriteSummary(s *Summary) error {
	f, err := os.OpenFile(filepath.Join(l.logDir, fmt.Sprintf("summary_%s.jsonl", s.ID)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(encode(s))
	return err
}

func extractStats(ollamaStats *parser.OllamaStats, usage *parser.OpenAIUsage) (int64, int64, float64, float64, float64, float64, int64, int64, int64) {
	if ollamaStats != nil {
		return ollamaStats.PromptEval, ollamaStats.EvalCount,
			ollamaStats.PromptEvalDurMs, ollamaStats.EvalDurMs,
			ollamaStats.LoadDurMs, ollamaStats.TotalsDuration, 0, 0, 0
	}
	if usage != nil {
		return 0, 0, 0, 0, 0, 0, usage.PromptTokens, usage.CompletionTokens, usage.TotalTokens
	}
	return 0, 0, 0, 0, 0, 0, 0, 0, 0
}

func encode(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
