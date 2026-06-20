package metrics

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
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
	data           sync.Map // string (modelName) -> *ModelUsage
	mu             sync.Mutex
	maxModels      int           // max models to retain; 0 = unlimited
	staleThreshold time.Duration // evict models with no new data after this duration
	cleanupTick    time.Duration // how often to run cleanup
	stopCh         chan struct{} // signal cleanup goroutine to stop

	// Track last access time per model for staleness detection.
	lastAccess sync.Map // string (modelName) -> int64 (unix nanos)
}

// NewModelUsageTracker creates a tracker with the given max models and stale threshold.
// A zero-value call defaults to 100 max models and 2h stale threshold.
func NewModelUsageTracker() *ModelUsageTracker {
	t := &ModelUsageTracker{
		maxModels:      100,
		staleThreshold: 2 * time.Hour,
		cleanupTick:    5 * time.Minute,
		stopCh:         make(chan struct{}),
	}
	go t.cleanupLoop()
	return t
}

// WithMaxModels sets the maximum number of models to retain.
func (t *ModelUsageTracker) WithMaxModels(n int) *ModelUsageTracker {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.maxModels = n
	return t
}

// Record adds usage data for the given model name.
func (t *ModelUsageTracker) Record(modelName string, promptTokens, completionTokens int64) {
	if modelName == "" {
		return
	}
	u := t.ensureModel(modelName)
	atomic.AddInt64(&u.PromptTokens, promptTokens)
	atomic.AddInt64(&u.CompletionTokens, completionTokens)
	atomic.AddInt64(&u.TotalTokens, promptTokens+completionTokens)
	atomic.AddInt64(&u.RequestCount, 1)
	t.touch(modelName)

	// Trigger periodic eviction if we exceed the limit.
	if t.maxModels > 0 {
		t.maybeEvict()
	}
}

// RecordRequest increments the request count for a model without changing token totals.
func (t *ModelUsageTracker) RecordRequest(modelName string) {
	if modelName == "" {
		return
	}
	u := t.ensureModel(modelName)
	atomic.AddInt64(&u.RequestCount, 1)
	t.touch(modelName)

	if t.maxModels > 0 {
		t.maybeEvict()
	}
}

// RecordTokens adds token usage for a model without incrementing request count.
func (t *ModelUsageTracker) RecordTokens(modelName string, promptTokens, completionTokens int64) {
	if modelName == "" {
		return
	}
	u := t.ensureModel(modelName)
	atomic.AddInt64(&u.PromptTokens, promptTokens)
	atomic.AddInt64(&u.CompletionTokens, completionTokens)
	atomic.AddInt64(&u.TotalTokens, promptTokens+completionTokens)
	t.touch(modelName)

	if t.maxModels > 0 {
		t.maybeEvict()
	}
}

func (t *ModelUsageTracker) ensureModel(modelName string) *ModelUsage {
	v, _ := t.data.LoadOrStore(modelName, &ModelUsage{})
	return v.(*ModelUsage)
}

func (t *ModelUsageTracker) touch(modelName string) {
	t.lastAccess.Store(modelName, time.Now().UnixNano())
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

// Totals returns aggregate token usage across all tracked models.
func (t *ModelUsageTracker) Totals() ModelUsage {
	if t == nil {
		return ModelUsage{}
	}

	var totals ModelUsage
	t.data.Range(func(key, value any) bool {
		u := value.(*ModelUsage)
		totals.PromptTokens += atomic.LoadInt64(&u.PromptTokens)
		totals.CompletionTokens += atomic.LoadInt64(&u.CompletionTokens)
		totals.TotalTokens += atomic.LoadInt64(&u.TotalTokens)
		totals.RequestCount += atomic.LoadInt64(&u.RequestCount)
		return true
	})

	return totals
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

// maybeEvict is called after Record to evict low-volume models if the limit is exceeded.
func (t *ModelUsageTracker) maybeEvict() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.maxModels <= 0 {
		return
	}

	count := 0
	t.data.Range(func(key, value any) bool {
		count++
		return true
	})
	if count <= t.maxModels {
		return
	}

	// Collect all entries and sort by request count.
	type entry struct {
		name string
		reqs int64
	}
	var entries []entry
	t.data.Range(func(key, value any) bool {
		k := key.(string)
		u := value.(*ModelUsage)
		entries = append(entries, entry{k, atomic.LoadInt64(&u.RequestCount)})
		return true
	})

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].reqs != entries[j].reqs {
			return entries[i].reqs > entries[j].reqs
		}
		return entries[i].name < entries[j].name
	})

	// Evict everything below the top N.
	for i := t.maxModels; i < len(entries); i++ {
		t.data.Delete(entries[i].name)
		t.lastAccess.Delete(entries[i].name)
	}
}

// cleanupLoop periodically removes stale models with zero activity and expired access timestamps.
func (t *ModelUsageTracker) cleanupLoop() {
	tick := time.NewTicker(t.cleanupTick)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			t.evictStale()
		case <-t.stopCh:
			return
		}
	}
}

// evictStale removes models that haven't been accessed within the stale threshold.
func (t *ModelUsageTracker) evictStale() {
	now := time.Now().UnixNano()
	t.data.Range(func(key, value any) bool {
		k := key.(string)
		lastNanos, loaded := t.lastAccess.Load(k)
		if !loaded {
			return true
		}
		elapsed := now - lastNanos.(int64)
		if elapsed > int64(t.staleThreshold) {
			t.data.Delete(k)
			t.lastAccess.Delete(k)
		}
		return true
	})
}

// Stop halts the cleanup goroutine. Call on shutdown.
func (t *ModelUsageTracker) Stop() {
	close(t.stopCh)
}
