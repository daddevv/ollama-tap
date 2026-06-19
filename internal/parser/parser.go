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

// OpenAIUsage holds usage stats from an OpenAI-compatible API response.
type OpenAIUsage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
}

// ParseOpenAIUsage extracts usage from a JSON object (e.g., SSE data).
func ParseOpenAIUsage(obj map[string]json.RawMessage) *OpenAIUsage {
	var raw struct {
		PromptTokens     *json.Number `json:"prompt_tokens"`
		CompletionTokens *json.Number `json:"completion_tokens"`
		TotalTokens      *json.Number `json:"total_tokens"`
	}
	for k, v := range obj {
		switch k {
		case "usage":
			var usage struct {
				PromptTokens     *json.Number `json:"prompt_tokens"`
				CompletionTokens *json.Number `json:"completion_tokens"`
				TotalTokens      *json.Number `json:"total_tokens"`
			}
			if err := json.Unmarshal(v, &usage); err == nil {
				raw = usage
			}
			continue
		}
	}

	u := &OpenAIUsage{}
	if raw.PromptTokens != nil {
		f, _ := raw.PromptTokens.Float64()
		u.PromptTokens = int64(f)
	}
	if raw.CompletionTokens != nil {
		f, _ := raw.CompletionTokens.Float64()
		u.CompletionTokens = int64(f)
	}
	if raw.TotalTokens != nil {
		f, _ := raw.TotalTokens.Float64()
		u.TotalTokens = int64(f)
	}

	if u.PromptTokens == 0 && u.CompletionTokens == 0 && u.TotalTokens == 0 {
		return nil
	}
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
	_, ok := obj["usage"]
	if ok {
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
	var obj struct {
		Usage *OpenAIUsage `json:"usage"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("parse OpenAI response: %w", err)
	}
	if obj.Usage == nil || (obj.Usage.PromptTokens == 0 && obj.Usage.TotalTokens == 0) {
		return nil, nil
	}
	return obj.Usage, nil
}

// ParseResponsesModel extracts the model name from /v1/responses or /v1/models data.
func ParseResponsesModel(obj map[string]json.RawMessage) string {
	if rawModel, ok := obj["model"]; ok {
		var s string
		json.Unmarshal(rawModel, &s)
		return s
	}
	return ""
}
