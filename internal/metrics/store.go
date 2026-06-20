package metrics

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const historyFileName = "history.jsonl"
const persistenceWindow = 2 * time.Hour

// RingStore maintains a ring buffer of periodic snapshots of cumulative counters.
type RingStore struct {
	buffer   []snapshotSlot
	head     atomic.Int64
	size     int64
	tickSec  int64
	stopped  chan struct{}
	mu       sync.RWMutex
	upstream *Metrics
	tracker  *ModelUsageTracker
	logDir   string
	persistFile *os.File
}

type snapshotSlot struct {
	timestamp        time.Time
	totalReqs        int64
	streaming        int64
	nonStreaming     int64
	failures         int64
	activeConns      int64
	promptTokens     int64
	completionTokens int64
}

// HistoryEntry is a delta snapshot suitable for chart rendering.
type HistoryEntry struct {
	Label            string    `json:"label"`
	Timestamp        time.Time `json:"timestamp"`
	BucketSeconds    int64     `json:"bucket_seconds"`
	TotalReqs        int64     `json:"total_reqs_delta"`
	Streaming        int64     `json:"streaming_delta"`
	NonStreaming     int64     `json:"non_streaming_delta"`
	Failures         int64     `json:"failures_delta"`
	Successful       int64     `json:"successful_delta"`
	PromptTokens     int64     `json:"prompt_tokens_delta"`
	CompletionTokens int64     `json:"completion_tokens_delta"`
}

// ModelData captures per-model token usage at a snapshot point.
type ModelData struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
	RequestCount     int64 `json:"request_count"`
}

// NewRingStore creates a RingStore backed by the given Metrics and ModelUsageTracker.
// Buffer holds 2 hours of data at 5-second intervals (1,440 slots).
func NewRingStore(m *Metrics, tracker *ModelUsageTracker, logDir string) *RingStore {
	s := &RingStore{
		buffer:   make([]snapshotSlot, 1440), // 2h × 3600s / 5s = 1440 slots
		size:     1440,
		tickSec:  5,
		stopped:  make(chan struct{}),
		upstream: m,
		tracker:  tracker,
		logDir:   logDir,
	}

	// Open (or create) the history file for persistence.
	path := filepath.Join(logDir, historyFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("ollama-tap: persist: failed to open %s: %v", path, err)
	} else {
		s.persistFile = f
	}

	// Load persisted history to survive restarts.
	entries, loadErr := loadHistory(logDir)
	if loadErr == nil && len(entries) > 0 {
		s.loadHistory(entries)
	}

	s.head.Store(int64(len(entries)) - 1)
	go s.ticker()
	return s
}

// ticker runs every tickSec seconds, recording cumulative values.
func (s *RingStore) ticker() {
	tick := time.NewTicker(time.Duration(s.tickSec) * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			s.tick()
		case <-s.stopped:
			return
		}
	}
}

func (s *RingStore) tick() {
	m := s.upstream
	var totals ModelUsage
	if s.tracker != nil {
		totals = s.tracker.Totals()
	}
	idx := ((s.head.Add(1)+s.size)%s.size + s.size) % s.size

	slot := snapshotSlot{
		timestamp:        time.Now().UTC(),
		totalReqs:        m.totalRequests.Load(),
		streaming:        int64(m.streamingCount.Load()),
		nonStreaming:     int64(m.nonStreamingCount.Load()),
		failures:         m.failedRequests.Load(),
		activeConns:      m.activeConnections.Load(),
		promptTokens:     totals.PromptTokens,
		completionTokens: totals.CompletionTokens,
	}

	s.mu.Lock()
	s.buffer[idx] = slot
	s.mu.Unlock()

	// Persist to disk for durability across restarts.
	if s.persistFile != nil {
		data, err := json.Marshal(slot)
		if err == nil {
			s.persistFile.Write(append(data, byte(10)))
		}
	}
}

// Stop halts the ticker goroutine and closes the persistence file. Call on shutdown.
func (s *RingStore) Stop() {
	close(s.stopped)
	s.StopPersist()
}

// Tick records a snapshot of the current metrics into the ring buffer synchronously.
func (s *RingStore) Tick() {
	s.tick()
}

// Snapshot returns the latest buffered slot with live active connections blended in.
func (s *RingStore) Snapshot() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	head := s.head.Load()
	var bufTS time.Time
	if head >= 0 {
		bufSlot := s.buffer[((head%int64(len(s.buffer)))+int64(len(s.buffer)))%int64(len(s.buffer))]
		bufTS = bufSlot.timestamp
	}
	return map[string]interface{}{
		"timestamp":          bufTS,
		"active_connections": s.upstream.activeConnections.Load(),
	}
}

// LatestSlot returns the latest buffered slot (caller must not mutate).
func (s *RingStore) LatestSlot() (snapshotSlot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	head := s.head.Load()
	if head < 0 {
		return snapshotSlot{}, false
	}
	slot := s.buffer[((head%int64(len(s.buffer)))+int64(len(s.buffer)))%int64(len(s.buffer))]
	if slot.timestamp.IsZero() {
		return snapshotSlot{}, false
	}
	return slot, true
}

// History returns snapshots for the last nMinutes as delta entries, in chronological order.
func (s *RingStore) History(nMinutes int) []HistoryEntry {
	if nMinutes <= 0 {
		return nil
	}
	count := nMinutes * 60 / int(s.tickSec)
	if count <= 0 {
		count = 1
	}
	if count > int(s.size) {
		count = int(s.size)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	head := s.head.Load()
	if head < 0 {
		return nil
	}
	slotLen := int64(len(s.buffer))
	validStart := head - int64(count) + 1
	var entries []snapshotSlot
	for i := validStart; i <= head; i++ {
		rawIdx := ((i % slotLen) + slotLen) % slotLen
		slot := s.buffer[rawIdx]
		if slot.timestamp.IsZero() {
			continue
		}
		entries = append(entries, slot)
	}

	deltaEntries := make([]HistoryEntry, 0, len(entries))
	for i, e := range entries {
		delta := HistoryEntry{
			Label:         e.timestamp.Format("15:04:05"),
			Timestamp:     e.timestamp,
			BucketSeconds: s.tickSec,
		}
		if i == 0 {
			deltaEntries = append(deltaEntries, delta)
			continue
		}

		prev := entries[i-1]
		if e.totalReqs < prev.totalReqs || e.streaming < prev.streaming || e.nonStreaming < prev.nonStreaming || e.failures < prev.failures || e.promptTokens < prev.promptTokens || e.completionTokens < prev.completionTokens {
			deltaEntries = append(deltaEntries, delta)
			continue
		}

		delta.TotalReqs = e.totalReqs - prev.totalReqs
		delta.Streaming = e.streaming - prev.streaming
		delta.NonStreaming = e.nonStreaming - prev.nonStreaming
		delta.Failures = e.failures - prev.failures
		delta.Successful = (e.totalReqs - e.failures) - (prev.totalReqs - prev.failures)
		if delta.Successful < 0 {
			delta.Successful = 0
		}
		delta.PromptTokens = e.promptTokens - prev.promptTokens
		delta.CompletionTokens = e.completionTokens - prev.completionTokens
		deltaEntries = append(deltaEntries, delta)
	}

	return deltaEntries
}

// HistorySeries returns history compressed into at most maxPoints buckets.
func (s *RingStore) HistorySeries(nMinutes, maxPoints int) []HistoryEntry {
	history := s.History(nMinutes)
	if maxPoints <= 0 || len(history) <= maxPoints {
		return history
	}

	bucketSize := (len(history) + maxPoints - 1) / maxPoints
	series := make([]HistoryEntry, 0, (len(history)+bucketSize-1)/bucketSize)
	for start := 0; start < len(history); start += bucketSize {
		end := start + bucketSize
		if end > len(history) {
			end = len(history)
		}

		bucket := HistoryEntry{
			Label:         history[end-1].Label,
			Timestamp:     history[end-1].Timestamp,
			BucketSeconds: int64(end-start) * s.tickSec,
		}
		for _, entry := range history[start:end] {
			bucket.TotalReqs += entry.TotalReqs
			bucket.Streaming += entry.Streaming
			bucket.NonStreaming += entry.NonStreaming
			bucket.Failures += entry.Failures
			bucket.Successful += entry.Successful
			bucket.PromptTokens += entry.PromptTokens
			bucket.CompletionTokens += entry.CompletionTokens
		}
		series = append(series, bucket)
	}

	return series
}


// loadHistory reads persisted snapshots from <logDir>/history.jsonl that fall within
// the persistence window and returns them in chronological order.
func loadHistory(logDir string) ([]snapshotSlot, error) {
	path := filepath.Join(logDir, historyFileName)
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := []string{}
	for _, line := range strings.Split(string(content), "\n") {
		if line == "" {
			continue
		}
		lines = append(lines, line)
	}

	var all []snapshotSlot
	for _, line := range lines {
		var slot snapshotSlot
		if err := json.Unmarshal([]byte(line), &slot); err != nil {
			continue // skip corrupt entries
		}
		all = append(all, slot)
	}

	cutoff := time.Now().Add(-persistenceWindow)
	var result []snapshotSlot
	for _, s := range all {
		if !s.timestamp.IsZero() && s.timestamp.After(cutoff) {
			result = append(result, s)
		}
	}
	return result, nil
}

// loadHistory fills the ring buffer with pre-loaded snapshot entries and updates head.
func (s *RingStore) loadHistory(entries []snapshotSlot) {
	if len(entries) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	slotLen := int64(len(s.buffer))
	for i, e := range entries {
		rawIdx := int64(i) % slotLen
		e.timestamp = e.timestamp.UTC()
		s.buffer[rawIdx] = e
	}
}

// StopPersist flushes and closes the history file.
func (s *RingStore) StopPersist() {
	if s.persistFile != nil {
		s.persistFile.Close()
		s.persistFile = nil
	}
}

// maxHistorySlots returns the maximum number of slots that fit in the ring buffer.
func (s *RingStore) maxHistorySlots() int64 {
	return s.size
}
