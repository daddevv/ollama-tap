package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

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
}

type snapshotSlot struct {
	timestamp    time.Time
	totalReqs    int64
	streaming    int64
	nonStreaming int64
	failures     int64
	activeConns  int64
}

// HistoryEntry is a delta snapshot suitable for chart rendering.
type HistoryEntry struct {
	Label        string    `json:"label"`
	Timestamp    time.Time `json:"timestamp"`
	TotalReqs    int64     `json:"total_reqs_delta"`
	Streaming    int64     `json:"streaming_delta"`
	NonStreaming int64     `json:"non_streaming_delta"`
	Failures     int64     `json:"failures_delta"`
	Successful   int64     `json:"successful_delta"`
}

// ModelData captures per-model token usage at a snapshot point.
type ModelData struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
	RequestCount     int64 `json:"request_count"`
}

// NewRingStore creates a RingStore backed by the given Metrics and ModelUsageTracker.
func NewRingStore(m *Metrics, tracker *ModelUsageTracker) *RingStore {
	s := &RingStore{
		buffer:   make([]snapshotSlot, 720), // ~60 min at 5s intervals
		size:     720,
		tickSec:  5,
		stopped:  make(chan struct{}),
		upstream: m,
		tracker:  tracker,
	}
	s.head.Store(-1)
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
	idx := s.head.Add(1) % s.size
	s.buffer[idx] = snapshotSlot{
		timestamp:    time.Now().UTC(),
		totalReqs:    m.totalRequests.Load(),
		streaming:    int64(m.streamingCount.Load()),
		nonStreaming: int64(m.nonStreamingCount.Load()),
		failures:     m.failedRequests.Load(),
		activeConns:  m.activeConnections.Load(),
	}
}

// Stop halts the ticker goroutine. Call on shutdown.
func (s *RingStore) Stop() {
	close(s.stopped)
}

// Tick records a snapshot of the current metrics into the ring buffer.
// Useful for tests that need to trigger ticks synchronously rather than waiting for the ticker.
func (s *RingStore) Tick() {
	s.tick()
}

// Snapshot returns the latest snapshot data as a dashboard-friendly map.
func (s *RingStore) Snapshot() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	head := s.head.Load()
	slot := s.buffer[((head%s.size)+s.size)%s.size]

	result := make(map[string]interface{})
	if slot.timestamp.IsZero() {
		result["timestamp"] = time.Now().UTC()
	} else {
		result["timestamp"] = slot.timestamp
	}
	result["active_connections"] = slot.activeConns
	return result
}

// History returns snapshots for the last nMinutes as delta entries, in chronological order.
func (s *RingStore) History(nMinutes int) []HistoryEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := nMinutes * 60 / int(s.tickSec)
	if count > int(s.size) {
		count = int(s.size)
	}

	head := s.head.Load()
	var entries []snapshotSlot
	for i := head - int64(count) + 1; i <= head; i++ {
		rawIdx := ((i % s.size) + s.size) % s.size
		slot := s.buffer[rawIdx]
		if slot.timestamp.IsZero() {
			continue
		}
		entries = append(entries, slot)
	}

	// Convert cumulative snapshots to delta entries.
	deltas := make([]HistoryEntry, 0, len(entries))
	for i, e := range entries {
		delta := HistoryEntry{
			Label:        e.timestamp.Format("15:04:05"),
			Timestamp:    e.timestamp,
			TotalReqs:    e.totalReqs,
			Streaming:    e.streaming,
			NonStreaming: e.nonStreaming,
			Failures:     e.failures,
			Successful:   e.totalReqs - e.failures,
		}
		if i > 0 {
			prev := entries[i-1]
			delta.TotalReqs = e.totalReqs - prev.totalReqs
			delta.Streaming = e.streaming - prev.streaming
			delta.NonStreaming = e.nonStreaming - prev.nonStreaming
			delta.Failures = e.failures - prev.failures
			delta.Successful = (e.totalReqs - e.failures) - (prev.totalReqs - prev.failures)
		}
		deltas = append(deltas, delta)
	}

	return deltas
}
