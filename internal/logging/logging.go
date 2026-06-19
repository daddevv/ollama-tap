package logging

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/daddevv/ollama-tap/internal/parser"
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
	ID      string              `json:"id"`
	Model   string              `json:"model,omitempty"`
	ReqType string              `json:"req_type"`
	Method  string              `json:"method"`
	Path    string              `json:"path"`
	URL     string              `json:"url"`
	Headers map[string][]string `json:"headers,omitempty"`
	Body    string              `json:"body,omitempty"`
	Time    string              `json:"time"`
}

func (l *Logger) WriteRequestLog(r *RequestLog) error {
	f, err := os.OpenFile(filepath.Join(l.logDir, fmt.Sprintf("request_%s.jsonl", r.ID)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := encode(r)
	if err != nil {
		return fmt.Errorf("marshal request log: %w", err)
	}
	_, err = f.Write(b)
	return err
}

type ResponsePreview struct {
	ID         string              `json:"id"`
	StatusCode int                 `json:"status_code"`
	Headers    map[string][]string `json:"headers,omitempty"`
	Body       string              `json:"body_preview,omitempty"`
	Time       string              `json:"time"`
}

func (l *Logger) WriteResponsePreview(p *ResponsePreview) error {
	f, err := os.OpenFile(filepath.Join(l.logDir, fmt.Sprintf("response_%s.jsonl", p.ID)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := encode(p)
	if err != nil {
		return fmt.Errorf("marshal response preview: %w", err)
	}
	_, err = f.Write(b)
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
	b, err := encode(c)
	if err != nil {
		return fmt.Errorf("marshal stream chunk: %w", err)
	}
	_, err = f.Write(append(b, '\n'))
	return err
}

type Summary struct {
	ID              string  `json:"id"`
	Model           string  `json:"model,omitempty"`
	ReqType         string  `json:"req_type"`
	Method          string  `json:"method"`
	Path            string  `json:"path"`
	DurationMs      float64 `json:"duration_ms"`
	UpstreamBytes   int64   `json:"uplink_bytes"`
	ClientBytes     int64   `json:"downlink_bytes"`
	PromptEvalCnt   int64   `json:"prompt_eval_count,omitempty"`
	EvalCnt         int64   `json:"eval_count,omitempty"`
	PromptEvalDurMs float64 `json:"prompt_eval_duration_ms,omitempty"`
	EvalDurMs       float64 `json:"eval_duration_ms,omitempty"`
	LoadDurMs       float64 `json:"load_duration_ms,omitempty"`
	TotalDurMs      float64 `json:"total_duration_ms,omitempty"`
	UsagePromptTok  int64   `json:"usage_prompt_tokens,omitempty"`
	UsageCompTok    int64   `json:"usage_completion_tokens,omitempty"`
	ErrType         string  `json:"error_type,omitempty"`
	UsageTotalTok   int64   `json:"usage_total_tokens,omitempty"`
	Time            string  `json:"time"`
}

func (l *Logger) WriteSummary(s *Summary) error {
	f, err := os.OpenFile(filepath.Join(l.logDir, fmt.Sprintf("summary_%s.jsonl", s.ID)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := encode(s)
	if err != nil {
		return fmt.Errorf("marshal summary: %w", err)
	}
	_, err = f.Write(b)
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

func encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Error represents an error or failure that occurred during request processing.
type Error struct {
	ID           string `json:"id"`
	Type         string `json:"error_type,omitempty"`
	ErrorMsg     string `json:"error_message,omitempty"`
	Time         string `json:"time"`
	UpstreamBytes int64  `json:"uplink_bytes,omitempty"`
}

// WriteError logs an error event to a dedicated error log file.
func (l *Logger) WriteError(e *Error) error {
	f, err := os.OpenFile(filepath.Join(l.logDir, fmt.Sprintf("error_%s.jsonl", e.ID)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal error record: %w", err)
	}
	_, writeErr := f.Write(append(b, '\n'))
	return writeErr
}

// WriteStreamChunks writes multiple stream chunks in a single file open.
func (l *Logger) WriteStreamChunks(chunks []*StreamChunk) error {
	f, err := os.OpenFile(filepath.Join(l.logDir, fmt.Sprintf("chunks_%s.jsonl", chunks[0].ID)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, c := range chunks {
		b, err := encode(c)
		if err != nil {
			return fmt.Errorf("marshal stream chunk: %w", err)
		}
		if _, err := f.Write(append(b, '\n')); err != nil {
			return err
		}
	}
	return nil
}
// WriteJournal writes multiple JSONL records to a single journal file,
// reducing per-request file-open overhead. Each record is written on its own line.
func (l *Logger) WriteJournal(records []any) error {
	f, err := os.OpenFile(filepath.Join(l.logDir, "journal.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, r := range records {
		b, err := encode(r)
		if err != nil {
			return fmt.Errorf("marshal journal record: %w", err)
		}
		b = append(b, '\n')
		if _, err := f.Write(b); err != nil {
			return err
		}
	}
	return nil
}
