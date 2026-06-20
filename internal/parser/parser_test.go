package parser

import (
	"encoding/json"
	"testing"
)

func TestParseOllamaJSONL(t *testing.T) {
	tests := []struct {
		name      string
		line      string
		wantDone  bool
		wantModel string
		wantEval  int64
		hasExtra  bool
		wantNil   bool
	}{
		{
			name:      "text chunk",
			line:      `{"model":"qwen3.6:latest","content":"Hello ","done":false}`,
			wantDone:  false,
			wantModel: "qwen3.6:latest",
			hasExtra:  true, // "content" is an extra field
		},
		{
			name:      "summary chunk",
			line:      `{"model":"qwen3.6:latest","done":true,"eval_count":42,"prompt_eval_count":10}`,
			wantDone:  true,
			wantModel: "qwen3.6:latest",
			wantEval:  42,
		},
		{
			name:      "extra fields",
			line:      `{"model":"qwen3.6","done":false,"content":"hi","extra_field":"val"}`,
			hasExtra:  true,
			wantModel: "qwen3.6", // model is extracted even with extra fields
		},
		{
			name:    "invalid json",
			line:    `{not valid json`,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStats, gotExtra := ParseOllamaJSONL(tt.line)
			if tt.wantNil {
				if gotStats != nil {
					t.Errorf("expected nil stats for invalid json")
				}
				return
			}
			if gotStats == nil {
				t.Fatalf("unexpected nil stats")
			}
			if gotStats.Done != tt.wantDone {
				t.Errorf("done = %v, want %v", gotStats.Done, tt.wantDone)
			}
			if gotStats.Model != tt.wantModel {
				t.Errorf("model = %q, want %q", gotStats.Model, tt.wantModel)
			}
			if gotExtra != tt.hasExtra {
				t.Errorf("hasExtra = %v, want %v", gotExtra, tt.hasExtra)
			}
			if gotStats.EvalCount != tt.wantEval {
				t.Errorf("eval_count = %d, want %d", gotStats.EvalCount, tt.wantEval)
			}
		})
	}
}

func TestParseOllamaJSONLDurations(t *testing.T) {
	line := `{"model":"qwen","done":true,"prompt_eval_duration":123456789000,"eval_duration":987654321000}`
	stats, _ := ParseOllamaJSONL(line)
	if stats.PromptEvalDurMs <= 0 {
		t.Errorf("expected positive prompt_eval_duration_ms, got %f", stats.PromptEvalDurMs)
	}
	if stats.EvalDurMs <= 0 {
		t.Errorf("expected positive eval_duration_ms, got %f", stats.EvalDurMs)
	}
}

func TestParseOpenAIUsage(t *testing.T) {
	raw := map[string]json.RawMessage{
		"id":     json.RawMessage(`"chatcmpl-abc"`),
		"object": json.RawMessage(`"chat.completion"`),
		"usage":  json.RawMessage(`{"prompt_tokens":10,"completion_tokens":42,"total_tokens":52}`),
	}

	usage := ParseOpenAIUsage(raw)
	if usage == nil {
		t.Fatal("expected non-nil usage")
	}
	if usage.PromptTokens != 10 {
		t.Errorf("prompt_tokens = %d, want 10", usage.PromptTokens)
	}
	if usage.CompletionTokens != 42 {
		t.Errorf("completion_tokens = %d, want 42", usage.CompletionTokens)
	}
	if usage.TotalTokens != 52 {
		t.Errorf("total_tokens = %d, want 52", usage.TotalTokens)
	}
}

func TestParseOpenAIUsageNoUsage(t *testing.T) {
	raw := map[string]json.RawMessage{
		"id": json.RawMessage(`"chatcmpl-abc"`),
	}
	got := ParseOpenAIUsage(raw)
	if got != nil {
		t.Errorf("expected nil usage, got %+v", got)
	}
}

func TestParseOpenAISSEEvent(t *testing.T) {
	obj, ok := ParseOpenAISSEEvent(`data: {"id":"test"}`)
	if !ok {
		t.Fatal("expected valid SSE event")
	}
	if raw, ok2 := obj["id"]; ok2 {
		var s string
		json.Unmarshal(raw, &s)
		if s != "test" {
			t.Errorf("got %q", s)
		}
	}

	_, ok = ParseOpenAISSEEvent(`event: chunk`)
	if ok {
		t.Error("expected invalid SSE event")
	}
}

func TestParseOpenAISSEEventArrayData(t *testing.T) {
	// SSE data that is a JSON array cannot be parsed as map[string]json.RawMessage
	_, ok := ParseOpenAISSEEvent(`data: [1,2,3]`)
	if ok {
		t.Error("expected invalid SSE event for array data (not a JSON object)")
	}

	obj, ok := ParseOpenAISSEEvent(`data: {"type":"text","text":"hello"}`)
	if !ok {
		t.Fatal("expected valid SSE event with object data")
	}
	rawText := obj["text"]
	var s string
	json.Unmarshal(rawText, &s)
	if s != "hello" {
		t.Errorf("text = %q, want %q", s, "hello")
	}
}

func TestExtractDeltaFromResponse(t *testing.T) {
	raw := map[string]json.RawMessage{
		"content": json.RawMessage(`[{"type":"text","text":"Hello world"}]`),
	}
	delta, _ := ExtractDelta(raw)
	if delta != "Hello world" {
		t.Errorf("delta = %q, want %q", delta, "Hello world")
	}
}

func TestExtractDeltaFromChoices(t *testing.T) {
	raw := map[string]json.RawMessage{
		"choices": json.RawMessage(`[{"delta":{"content":"Hi there"}}]`),
	}
	delta, _ := ExtractDelta(raw)
	if delta != "Hi there" {
		t.Errorf("delta = %q, want %q", delta, "Hi there")
	}
}

func TestIsUsageEvent(t *testing.T) {
	raw1 := map[string]json.RawMessage{
		"usage": json.RawMessage(`{"prompt_tokens":10}`),
	}
	if !IsUsageEvent(raw1) {
		t.Error("expected usage event")
	}

	raw2 := map[string]json.RawMessage{
		"choices": json.RawMessage(`[{"finish_reason":"stop"}]`),
	}
	if !IsUsageEvent(raw2) {
		t.Error("expected usage event (finish_reason=stop)")
	}

	raw3 := map[string]json.RawMessage{
		"id": json.RawMessage(`"chatcmpl-abc"`),
	}
	if IsUsageEvent(raw3) {
		t.Error("expected no usage event")
	}
}

func TestParseOpenAIChatNonStreaming(t *testing.T) {
	body := []byte(`{"id":"chatcmpl-123","object":"chat.completion","usage":{"prompt_tokens":10,"completion_tokens":42,"total_tokens":52}}`)
	usage, err := ParseOpenAIChatNonStreaming(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if usage == nil || usage.TotalTokens != 52 {
		t.Errorf("unexpected usage: %+v", usage)
	}

	body2 := []byte(`{"id":"chatcmpl-123","object":"chat.completion","usage":null}`)
	usage2, err2 := ParseOpenAIChatNonStreaming(body2)
	if err2 != nil {
		t.Fatalf("unexpected error: %v", err2)
	}
	if usage2 != nil {
		t.Errorf("expected nil usage for null usage field")
	}
}

func TestParseOllamaNonStreaming(t *testing.T) {
	body := []byte(`{"model":"qwen3.6","done":true,"eval_count":15,"prompt_eval_count":8}`)
	stats, err := ParseOllamaNonStreaming(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !stats.Done || stats.EvalCount != 15 {
		t.Errorf("expected done=true and eval_count=15")
	}
}

func TestParseResponsesModel(t *testing.T) {
	raw := map[string]json.RawMessage{
		"model": json.RawMessage(`"qwen3.6:latest"`),
	}
	got := ParseResponsesModel(raw)
	if got != "qwen3.6:latest" {
		t.Errorf("model = %q, want %q", got, "qwen3.6:latest")
	}

	raw2 := map[string]json.RawMessage{}
	got2 := ParseResponsesModel(raw2)
	if got2 != "" {
		t.Errorf("expected empty model, got %q", got2)
	}
}

func TestParseOllamaSSEUsage(t *testing.T) {
	t.Run("openai-compatible fields", func(t *testing.T) {
		obj := map[string]json.RawMessage{
			"usage": json.RawMessage(`{"prompt_tokens":10,"completion_tokens":42}`),
		}
		u := ParseOllamaSSEUsage(obj)
		if u == nil {
			t.Fatal("expected usage")
		}
		if u.PromptTokens != 10 {
			t.Errorf("prompt_tokens = %d, want 10", u.PromptTokens)
		}
		if u.CompletionTokens != 42 {
			t.Errorf("completion_tokens = %d, want 42", u.CompletionTokens)
		}
	})

	t.Run("native nanosecond fields", func(t *testing.T) {
		obj := map[string]json.RawMessage{
			"usage": json.RawMessage(`{"prompt_ns":100,"completion_ns":42}`),
		}
		u := ParseOllamaSSEUsage(obj)
		if u == nil {
			t.Fatal("expected usage")
		}
		if u.PromptTokens != 100 {
			t.Errorf("prompt_tokens = %d, want 100", u.PromptTokens)
		}
		if u.CompletionTokens != 42 {
			t.Errorf("completion_tokens = %d, want 42", u.CompletionTokens)
		}
	})

	t.Run("no usage field", func(t *testing.T) {
		obj := map[string]json.RawMessage{
			"choices": json.RawMessage(`[{"delta":{"content":"hi"}}]`),
		}
		u := ParseOllamaSSEUsage(obj)
		if u != nil {
			t.Errorf("expected nil usage, got %+v", u)
		}
	})
}

func TestParseOllamaSSEUsagePrecedence(t *testing.T) {
	t.Run("OpenAI fields take precedence over alias", func(t *testing.T) {
		obj := map[string]json.RawMessage{
			"usage": json.RawMessage(`{"prompt_tokens":5,"completion_tokens":3,"input_tokens":10,"output_tokens":4}`),
		}
		u := ParseOllamaSSEUsage(obj)
		if u == nil {
			t.Fatal("expected usage")
		}
		if u.PromptTokens != 5 {
			t.Errorf("prompt_tokens = %d, want 5", u.PromptTokens)
		}
		if u.CompletionTokens != 3 {
			t.Errorf("completion_tokens = %d, want 3", u.CompletionTokens)
		}
		// Alias fields should NOT override OpenAI fields when present
		if u.InputTokens != 0 {
			t.Errorf("input_tokens should be 0 (OpenAI prompt_tokens takes precedence), got %d", u.InputTokens)
		}
		if u.OutputTokens != 0 {
			t.Errorf("output_tokens should be 0 (OpenAI completion_tokens takes precedence), got %d", u.OutputTokens)
		}
	})

	t.Run("alias fields used when OpenAI fields absent", func(t *testing.T) {
		obj := map[string]json.RawMessage{
			"usage": json.RawMessage(`{"input_tokens":15,"output_tokens":8}`),
		}
		u := ParseOllamaSSEUsage(obj)
		if u == nil {
			t.Fatal("expected usage")
		}
		// Alias fields should be populated since no OpenAI fields exist
		if u.InputTokens != 15 {
			t.Errorf("input_tokens = %d, want 15", u.InputTokens)
		}
		if u.OutputTokens != 8 {
			t.Errorf("output_tokens = %d, want 8", u.OutputTokens)
		}
	})

	t.Run("OpenAI fields take precedence over nanosecond fallback", func(t *testing.T) {
		obj := map[string]json.RawMessage{
			"usage": json.RawMessage(`{"prompt_tokens":7,"completion_tokens":2,"prompt_ns":999,"completion_ns":888}`),
		}
		u := ParseOllamaSSEUsage(obj)
		if u == nil {
			t.Fatal("expected usage")
		}
		if u.PromptTokens != 7 {
			t.Errorf("prompt_tokens = %d, want 7", u.PromptTokens)
		}
		if u.CompletionTokens != 2 {
			t.Errorf("completion_tokens = %d, want 2", u.CompletionTokens)
		}
	})

	t.Run("nanosecond fallback when no token fields present", func(t *testing.T) {
		obj := map[string]json.RawMessage{
			"usage": json.RawMessage(`{"prompt_ns":100,"completion_ns":200}`),
		}
		u := ParseOllamaSSEUsage(obj)
		if u == nil {
			t.Fatal("expected usage")
		}
		if u.PromptTokens != 100 {
			t.Errorf("prompt_tokens (from ns) = %d, want 100", u.PromptTokens)
		}
		if u.CompletionTokens != 200 {
			t.Errorf("completion_tokens (from ns) = %d, want 200", u.CompletionTokens)
		}
	})

	t.Run("explicit zero OpenAI fields excludes alias fallback", func(t *testing.T) {
		// When prompt_tokens/completion_tokens are explicitly in the JSON (even as 0),
		// they take precedence and alias fields are not used.
		obj := map[string]json.RawMessage{
			"usage": json.RawMessage(`{"prompt_tokens":0,"completion_tokens":0,"input_tokens":20,"output_tokens":12}`),
		}
		u := ParseOllamaSSEUsage(obj)
		// Returns nil because all usable values are zero (OpenAI fields present but zero,
		// alias excluded due to precedence, no nanosecond fallback available)
		if u != nil {
			t.Errorf("expected nil usage when OpenAI fields explicit zero with no alias fallback, got %+v", u)
		}
	})
}

func TestParseResponsesModelNestedResponse(t *testing.T) {
	raw := map[string]json.RawMessage{
		"response": json.RawMessage(`{"model":"qwen3.6:latest","usage":null}`),
	}
	got := ParseResponsesModel(raw)
	if got != "qwen3.6:latest" {
		t.Errorf("model = %q, want %q", got, "qwen3.6:latest")
	}
}

func TestExtractUsageFromResponsesEventNestedAlias(t *testing.T) {
	raw := map[string]json.RawMessage{
		"type": json.RawMessage(`"response.completed"`),
		"response": json.RawMessage(`{
			"model":"qwen3.6:latest",
			"usage":{
				"input_tokens":20,
				"input_tokens_details":{"cached_tokens":0},
				"output_tokens":16,
				"output_tokens_details":{"reasoning_tokens":0},
				"total_tokens":36
			}
		}`),
	}

	usage := ExtractUsageFromResponsesEvent(raw)
	if usage == nil {
		t.Fatal("expected nested usage")
	}
	normalized := NormalizeUsage(usage)
	if normalized.InputTokens != 20 {
		t.Errorf("input tokens = %d, want 20", normalized.InputTokens)
	}
	if normalized.OutputTokens != 16 {
		t.Errorf("output tokens = %d, want 16", normalized.OutputTokens)
	}
	if normalized.TotalTokens != 36 {
		t.Errorf("total tokens = %d, want 36", normalized.TotalTokens)
	}
	if !normalized.UsageReliable {
		t.Error("expected reliable usage")
	}

	parsed := ParseOpenAIUsage(raw)
	if parsed == nil {
		t.Fatal("expected ParseOpenAIUsage to normalize nested alias usage")
	}
	if parsed.PromptTokens != 20 {
		t.Errorf("prompt_tokens = %d, want 20", parsed.PromptTokens)
	}
	if parsed.CompletionTokens != 16 {
		t.Errorf("completion_tokens = %d, want 16", parsed.CompletionTokens)
	}
	if parsed.TotalTokens != 36 {
		t.Errorf("total_tokens = %d, want 36", parsed.TotalTokens)
	}
}

func TestParseOpenAIChatNonStreamingAliasOnly(t *testing.T) {
	body := []byte(`{"id":"resp_123","object":"response","model":"qwen3.6:latest","usage":{"input_tokens":7,"output_tokens":9,"total_tokens":16}}`)
	usage, err := ParseOpenAIChatNonStreaming(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if usage == nil {
		t.Fatal("expected alias-only usage")
	}
	if usage.PromptTokens != 7 {
		t.Errorf("prompt_tokens = %d, want 7", usage.PromptTokens)
	}
	if usage.CompletionTokens != 9 {
		t.Errorf("completion_tokens = %d, want 9", usage.CompletionTokens)
	}
	if usage.TotalTokens != 16 {
		t.Errorf("total_tokens = %d, want 16", usage.TotalTokens)
	}
}
