package parser

import (
	"encoding/json"
	"fmt"
	"strings"
)

// OllamaStats holds per-request statistics from Ollama native API fields.
type OllamaStats struct {
	Model           string
	Done            bool
	PromptEval      int64   `json:"prompt_eval_count,omitempty"`
	EvalCount       int64   `json:"eval_count,omitempty"`
	PromptEvalDurMs float64 `json:"prompt_eval_duration_ms,omitempty"`
	EvalDurMs       float64 `json:"eval_duration_ms,omitempty"`
	LoadDurMs       float64 `json:"load_duration_ms,omitempty"`
	TotalsDuration  float64 `json:"total_duration,omitempty"`
}

// ParseOllamaJSONL attempts to parse a line of Ollama native JSONL stream data.
// Returns the parsed stats and whether there was any remaining text after the JSON.
func ParseOllamaJSONL(line string) (*OllamaStats, bool) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		return nil, true
	}

	stats := &OllamaStats{}
	for k, v := range raw {
		switch k {
		case "model":
			json.Unmarshal(v, &stats.Model)
		case "done":
			json.Unmarshal(v, &stats.Done)
		case "prompt_eval_count":
			json.Unmarshal(v, &stats.PromptEval)
		case "eval_count":
			json.Unmarshal(v, &stats.EvalCount)
		case "prompt_eval_duration":
			var rawDur json.Number
			if err := json.Unmarshal(v, &rawDur); err == nil {
				stats.PromptEvalDurMs = toMillis(rawDur)
			}
		case "eval_duration":
			var rawDur json.Number
			if err := json.Unmarshal(v, &rawDur); err == nil {
				stats.EvalDurMs = toMillis(rawDur)
			}
		case "load_duration":
			var rawDur json.Number
			if err := json.Unmarshal(v, &rawDur); err == nil {
				stats.LoadDurMs = toMillis(rawDur)
			}
		case "total_duration":
			var rawDur json.Number
			if err := json.Unmarshal(v, &rawDur); err == nil {
				stats.TotalsDuration = toMillis(rawDur)
			}
		}
	}

	var remaining strings.Builder
	for k, v := range raw {
		// skip known fields we already extracted
		switch k {
		case "model", "done", "prompt_eval_count", "eval_count":
			continue
		}
		if len(v) > 0 {
			remaining.WriteString(fmt.Sprintf("%s:%s ", k, string(v)))
		}
	}

	return stats, strings.TrimSpace(remaining.String()) != ""
}

func toMillis(n json.Number) float64 {
	f, err := n.Float64()
	if err != nil {
		return 0
	}
	return f / 1e6 // nanoseconds -> milliseconds
}

// OllamaSSEUsage holds usage stats from an SSE event that uses native
// Ollama nanosecond fields (prompt_ns / completion_ns) or token fields.
type OllamaSSEUsage struct {
	PromptTokens     int64
	CompletionTokens int64
	InputTokens      int64
	OutputTokens     int64
}

// ParseOllamaSSEUsage extracts usage from a JSON data object. It handles:
// - OpenAI fields: prompt_tokens, completion_tokens
// - Ollama /v1/responses alias fields: input_tokens, output_tokens
// - Native Ollama nanosecond fields: prompt_ns, completion_ns
// Returns nil when no usable values are found so the caller can distinguish "no usage" from "all zeroes".
func ParseOllamaSSEUsage(obj map[string]json.RawMessage) *OllamaSSEUsage {
	var inner struct {
		PromptTokens     *json.Number `json:"prompt_tokens"`
		CompletionTokens *json.Number `json:"completion_tokens"`
		InputTokens      *json.Number `json:"input_tokens"`
		OutputTokens     *json.Number `json:"output_tokens"`
		PromptNs         *json.Number `json:"prompt_ns"`
		CompletionNs     *json.Number `json:"completion_ns"`
	}

	// 1) Check if there is a wrapped "usage" object.
	for k, v := range obj {
		if k != "usage" {
			continue
		}
		var usage struct {
			PromptTokens     *json.Number `json:"prompt_tokens"`
			CompletionTokens *json.Number `json:"completion_tokens"`
			InputTokens      *json.Number `json:"input_tokens"`
			OutputTokens     *json.Number `json:"output_tokens"`
			PromptNs         *json.Number `json:"prompt_ns"`
			CompletionNs     *json.Number `json:"completion_ns"`
		}
		if err := json.Unmarshal(v, &usage); err != nil {
			continue
		}
		inner.PromptTokens = usage.PromptTokens
		inner.CompletionTokens = usage.CompletionTokens
		inner.InputTokens = usage.InputTokens
		inner.OutputTokens = usage.OutputTokens
		inner.PromptNs = usage.PromptNs
		inner.CompletionNs = usage.CompletionNs
	}

	u := &OllamaSSEUsage{}
	// Try OpenAI-compatible field names first (prompt_tokens/completion_tokens).
	if inner.PromptTokens != nil {
		f, _ := inner.PromptTokens.Float64()
		u.PromptTokens = int64(f)
	}
	if inner.CompletionTokens != nil {
		f, _ := inner.CompletionTokens.Float64()
		u.CompletionTokens = int64(f)
	}
	// Try Ollama /v1/responses alias fields (input_tokens/output_tokens).
	// Only populated when OpenAI prompt_tokens/completion_tokens fields are absent (nil),
	// to preserve precedence of OpenAI fields for reporting.
	if inner.InputTokens != nil && inner.PromptTokens == nil {
		f, _ := inner.InputTokens.Float64()
		u.InputTokens = int64(f)
	}
	if inner.OutputTokens != nil && inner.CompletionTokens == nil {
		f, _ := inner.OutputTokens.Float64()
		u.OutputTokens = int64(f)
	}
	// Fallback to native Ollama nanosecond fields when token fields are absent.
	if u.PromptTokens == 0 && u.InputTokens == 0 && inner.PromptNs != nil {
		f, _ := inner.PromptNs.Float64()
		u.PromptTokens = int64(f)
	}
	if u.CompletionTokens == 0 && u.OutputTokens == 0 && inner.CompletionNs != nil {
		f, _ := inner.CompletionNs.Float64()
		u.CompletionTokens = int64(f)
	}
	if u.PromptTokens == 0 && u.CompletionTokens == 0 && u.InputTokens == 0 && u.OutputTokens == 0 {
		return nil
	}
	return u
}

// OpenAIUsage holds usage stats from an OpenAI-compatible API response.
// Supports both OpenAI fields (prompt_tokens/completion_tokens) and
// Ollama /v1/responses alias fields (input_tokens/output_tokens).
type OpenAIUsage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
	InputTokens      int64 `json:"input_tokens"`
	OutputTokens     int64 `json:"output_tokens"`
}

// ParseOpenAIUsage extracts usage from a JSON object (e.g., SSE data).
// Handles both OpenAI fields (prompt_tokens/completion_tokens) and
// Ollama /v1/responses alias fields (input_tokens/output_tokens).
func ParseOpenAIUsage(obj map[string]json.RawMessage) *OpenAIUsage {
	u := ExtractUsageFromResponsesEvent(obj)
	normalized := NormalizeUsage(u)
	if !normalized.UsageReliable {
		return nil
	}
	u.PromptTokens = normalized.InputTokens
	u.CompletionTokens = normalized.OutputTokens
	u.TotalTokens = normalized.TotalTokens
	return u
}

// ParseOpenAISSEEvent extracts the JSON data from an SSE "data:" line.
func ParseOpenAISSEEvent(line string) (map[string]json.RawMessage, bool) {
	if !strings.HasPrefix(line, "data: ") {
		return nil, false
	}
	data := strings.TrimPrefix(line, "data: ")
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(data), &obj); err != nil {
		return nil, false
	}
	return obj, true
}

// ExtractDelta extracts the assistant message delta from an SSE data object.
func ExtractDelta(obj map[string]json.RawMessage) (string, int64) {
	// Ollama /v1/responses format: content is an array of objects with type="text" and text fields
	if rawContent, ok := obj["content"]; ok {
		var items []struct {
			Type string          `json:"type"`
			Text json.RawMessage `json:"text"`
		}
		if err := json.Unmarshal(rawContent, &items); err == nil {
			for _, item := range items {
				if item.Type == "text" && len(item.Text) > 0 {
					var s string
					if err := json.Unmarshal(item.Text, &s); err == nil && s != "" {
						return s, 0
					}
				}
			}
		}
	}

	// OpenAI /v1/chat/completions SSE format: choices[index].delta.content
	if rawChoices, ok := obj["choices"]; ok {
		var choices []struct {
			Delta struct {
				Content string `json:"content"`
			} `json:"delta"`
		}
		if err := json.Unmarshal(rawChoices, &choices); err == nil && len(choices) > 0 {
			return choices[0].Delta.Content, 0
		}
	}

	return "", 0
}

// IsUsageEvent checks if an SSE data object contains a usage field.
func IsUsageEvent(obj map[string]json.RawMessage) bool {
	if ExtractUsageFromResponsesEvent(obj) != nil {
		return true
	}
	if rawChoices, ok2 := obj["choices"]; ok2 {
		var choices []struct {
			FinishReason string `json:"finish_reason"`
		}
		json.Unmarshal(rawChoices, &choices)
		for _, c := range choices {
			if c.FinishReason == "stop" || c.FinishReason == "length" {
				return true
			}
		}
	}
	return false
}

// ParseOllamaNonStreaming extracts model and stats from a non-streaming /api/chat response.
func ParseOllamaNonStreaming(raw []byte) (*OllamaStats, error) {
	var rawMsg map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rawMsg); err != nil {
		return nil, fmt.Errorf("parse Ollama response: %w", err)
	}
	stats := &OllamaStats{}
	for k, v := range rawMsg {
		switch k {
		case "model":
			json.Unmarshal(v, &stats.Model)
		case "done":
			json.Unmarshal(v, &stats.Done)
		case "prompt_eval_count":
			json.Unmarshal(v, &stats.PromptEval)
		case "eval_count":
			json.Unmarshal(v, &stats.EvalCount)
		}
	}
	return stats, nil
}

// ParseOpenAIChatNonStreaming extracts usage from a non-streaming /v1/chat/completions response.
func ParseOpenAIChatNonStreaming(raw []byte) (*OpenAIUsage, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("parse OpenAI response: %w", err)
	}
	usage := ExtractUsageFromResponsesEvent(obj)
	normalized := NormalizeUsage(usage)
	if !normalized.UsageReliable {
		return nil, nil
	}
	usage.PromptTokens = normalized.InputTokens
	usage.CompletionTokens = normalized.OutputTokens
	usage.TotalTokens = normalized.TotalTokens
	return usage, nil
}

// NormalizedUsage provides unified token representation with metadata about token reliability.
type NormalizedUsage struct {
	InputTokens   int64
	OutputTokens  int64
	TotalTokens   int64
	UsagePresent  bool   // true if any usage object was found
	UsageReliable bool   // true if totalTokens > 0, indicating model returned meaningful token counts
	Source        string // "openai", "ollama_alias", or "missing"
}

// NormalizeUsage converts raw usage data to normalized form.
// Handles OpenAI fields (prompt_tokens/completion_tokens) and
// Ollama alias fields (input_tokens/output_tokens) with precedence rules.
// Returns NormalizedUsage with metadata about reliability and source.
func NormalizeUsage(usage *OpenAIUsage) NormalizedUsage {
	if usage == nil {
		return NormalizedUsage{
			InputTokens:   0,
			OutputTokens:  0,
			TotalTokens:   0,
			UsagePresent:  false,
			UsageReliable: false,
			Source:        "missing",
		}
	}

	// Apply token field precedence: OpenAI fields take priority over Ollama alias fields
	inputTokens := usage.PromptTokens
	if inputTokens == 0 {
		inputTokens = usage.InputTokens
	}

	outputTokens := usage.CompletionTokens
	if outputTokens == 0 {
		outputTokens = usage.OutputTokens
	}

	totalTokens := usage.TotalTokens
	if totalTokens == 0 {
		totalTokens = inputTokens + outputTokens
	}

	// Determine source based on which fields were populated
	source := "missing"
	if usage.PromptTokens > 0 || usage.CompletionTokens > 0 {
		source = "openai"
	} else if usage.InputTokens > 0 || usage.OutputTokens > 0 {
		source = "ollama_alias"
	}

	return NormalizedUsage{
		InputTokens:   inputTokens,
		OutputTokens:  outputTokens,
		TotalTokens:   totalTokens,
		UsagePresent:  true,
		UsageReliable: totalTokens > 0,
		Source:        source,
	}
}

// ExtractUsageFromResponsesEvent extracts usage from a /v1/responses streaming event.
// Handles nested usage under event.usage or event.response.usage.
func ExtractUsageFromResponsesEvent(event map[string]json.RawMessage) *OpenAIUsage {
	// Try top-level usage field first
	if rawUsage, ok := event["usage"]; ok {
		if usage := parseOpenAIUsageRaw(rawUsage); usage != nil {
			return usage
		}
	}

	// Try nested response.usage for some API formats
	if rawResponse, ok := event["response"]; ok {
		var resp struct {
			Usage *OpenAIUsage `json:"usage"`
		}
		if err := json.Unmarshal(rawResponse, &resp); err == nil && resp.Usage != nil {
			return resp.Usage
		}
	}

	return nil
}

// ParseResponsesModel extracts the model name from /v1/responses or /v1/models data.
func ParseResponsesModel(obj map[string]json.RawMessage) string {
	if rawModel, ok := obj["model"]; ok {
		var s string
		json.Unmarshal(rawModel, &s)
		return s
	}
	if rawResponse, ok := obj["response"]; ok {
		var resp struct {
			Model string `json:"model"`
		}
		if err := json.Unmarshal(rawResponse, &resp); err == nil {
			return resp.Model
		}
	}
	return ""
}

func parseOpenAIUsageRaw(raw json.RawMessage) *OpenAIUsage {
	var usage *OpenAIUsage
	if err := json.Unmarshal(raw, &usage); err != nil || usage == nil {
		return nil
	}
	return usage
}
