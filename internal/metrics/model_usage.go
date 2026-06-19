package metrics

import (
	"sync"
	"sync/atomic"
)

// ModelUsage tracks token consumption per model name.
type ModelUsage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
	RequestCount     int64 `json:"request_count"`
}

// ModelUsageTracker accumulates per-model usage counts across all requests.
// Thread-safe: uses sync.Map with atomic fields for lock-free concurrent reads.
type ModelUsageTracker struct {
	data sync.Map // string (modelName) -> *ModelUsage
}

func NewModelUsageTracker() *ModelUsageTracker {
	return &ModelUsageTracker{}
}

// Record adds usage data for the given model name.
func (t *ModelUsageTracker) Record(modelName string, promptTokens, completionTokens int64) {
	if modelName == "" {
		return
	}
	v, _ := t.data.LoadOrStore(modelName, &ModelUsage{})
	u := v.(*ModelUsage)
	atomic.AddInt64(&u.PromptTokens, promptTokens)
	atomic.AddInt64(&u.CompletionTokens, completionTokens)
	atomic.AddInt64(&u.TotalTokens, promptTokens+completionTokens)
	atomic.AddInt64(&u.RequestCount, 1)
}

// Snapshot returns a copy of all tracked model usage.
func (t *ModelUsageTracker) Snapshot() map[string]ModelUsage {
	result := make(map[string]ModelUsage)
	t.data.Range(func(key, value any) bool {
		k := key.(string)
		u := value.(*ModelUsage)
		result[k] = ModelUsage{
			PromptTokens:     atomic.LoadInt64(&u.PromptTokens),
			CompletionTokens: atomic.LoadInt64(&u.CompletionTokens),
			TotalTokens:      atomic.LoadInt64(&u.TotalTokens),
			RequestCount:     atomic.LoadInt64(&u.RequestCount),
		}
		return true
	})
	return result
}

// HasRecords reports whether any model usage has been recorded.
func (t *ModelUsageTracker) HasRecords() bool {
	found := false
	t.data.Range(func(key, value any) bool {
		found = true
		return false
	})
	return found
}
