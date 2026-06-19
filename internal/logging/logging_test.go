package logging

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteRequestLog(t *testing.T) {
	dir := t.TempDir()
	l := New(dir)

	id := RecordID()
	r := &RequestLog{
		ID:     id,
		Model:  "qwen3.6",
		ReqType: "openai_chat",
		Method: "POST",
		Path:   "/v1/chat/completions",
		Time:   "2024-01-01T00:00:00Z",
	}

	if err := l.WriteRequestLog(r); err != nil {
		t.Fatalf("WriteRequestLog error: %v", err)
	}

	files, _ := os.ReadDir(dir)
	found := false
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "request_") {
			data, _ := os.ReadFile(filepath.Join(dir, f.Name()))
			var m map[string]any
			json.Unmarshal(data, &m)
			if m["id"] != id || m["method"] != "POST" {
				t.Errorf("unexpected request log: %v", m)
			}
			found = true
		}
	}
	if !found {
		t.Error("expected to find a request_*.jsonl file")
	}
}

func TestWriteStreamChunk(t *testing.T) {
	dir := t.TempDir()
	l := New(dir)

	id := RecordID()
	c := &StreamChunk{
		ID:        id,
		ChunkType: "text",
		Model:     "qwen3.6",
		Delta:     "Hello",
		Done:      false,
		Time:      "2024-01-01T00:00:00Z",
	}

	if err := l.WriteStreamChunk(c); err != nil {
		t.Fatalf("WriteStreamChunk error: %v", err)
	}

	data, _ := os.ReadFile(filepath.Join(dir, "chunks_"+id+".jsonl"))
	var m map[string]any
	json.Unmarshal(data, &m)
	if m["chunk_type"] != "text" || m["delta"] != "Hello" {
		t.Errorf("unexpected chunk: %v", m)
	}
}

func TestWriteSummary(t *testing.T) {
	dir := t.TempDir()
	l := New(dir)

	id := RecordID()
	s := &Summary{
		ID:           id,
		Model:        "qwen3.6",
		ReqType:      "ollama_native",
		Method:       "POST",
		Path:         "/api/chat",
		DurationMs:   1234.5,
		UpstreamBytes: 100,
		ClientBytes:  42,
		Time:         "2024-01-01T00:00:00Z",
	}

	if err := l.WriteSummary(s); err != nil {
		t.Fatalf("WriteSummary error: %v", err)
	}

	data, _ := os.ReadFile(filepath.Join(dir, "summary_"+id+".jsonl"))
	var m map[string]any
	json.Unmarshal(data, &m)
	if m["duration_ms"] != 1234.5 {
		t.Errorf("unexpected summary: %v", m)
	}
}

func TestWriteSummaryWithUsage(t *testing.T) {
	dir := t.TempDir()
	l := New(dir)

	id := RecordID()
	s := &Summary{
		ID:              id,
		Model:           "qwen3.6",
		ReqType:         "openai_chat",
		Method:          "POST",
		Path:            "/v1/chat/completions",
		DurationMs:      1234.5,
		UpstreamBytes:   100,
		ClientBytes:     42,
		PromptEvalCnt:   10,
		EvalCnt:         42,
		UsagePromptTok:  10,
		UsageCompTok:    42,
		UsageTotalTok:   52,
		Time:            "2024-01-01T00:00:00Z",
	}

	if err := l.WriteSummary(s); err != nil {
		t.Fatalf("WriteSummary error: %v", err)
	}

	data, _ := os.ReadFile(filepath.Join(dir, "summary_"+id+".jsonl"))
	var m map[string]any
	json.Unmarshal(data, &m)
	if int64(m["usage_total_tokens"].(float64)) != 52 {
		t.Errorf("unexpected total tokens: %v", m)
	}
	if int64(m["eval_count"].(float64)) != 42 {
		t.Errorf("unexpected eval count: %v", m)
	}
}

func TestRecordIDUniqueness(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := RecordID()
		if ids[id] {
			t.Errorf("duplicate ID: %s", id)
		}
		ids[id] = true
	}
}
