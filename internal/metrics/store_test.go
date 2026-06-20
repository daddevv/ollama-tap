package metrics

import (
	"testing"
	"time"
)

func TestRingStore_Snapshot_Early(t *testing.T) {
	m := New()
	store := NewRingStore(m, nil)
	defer store.Stop()

	snap := store.Snapshot()
	if snap["active_connections"] == nil {
		t.Fatal("snapshot missing active_connections")
	}
	if _, ok := snap["timestamp"].(time.Time); !ok {
		t.Fatal("snapshot timestamp is not a time.Time")
	}
}

func TestRingStore_History_Early(t *testing.T) {
	m := New()
	store := NewRingStore(m, nil)
	defer store.Stop()

	hist := store.History(1)
	if len(hist) != 0 {
		t.Fatalf("expected empty history before any ticks, got %d entries", len(hist))
	}
}

func TestRingStore_History_AfterTicks(t *testing.T) {
	m := New()
	tracker := NewModelUsageTracker()
	defer tracker.Stop()
	store := NewRingStore(m, tracker)
	defer store.Stop()

	// Trigger a tick so history has data
	store.Tick()

	m.RecordRequest(10*time.Millisecond, false)
	tracker.Record("qwen", 11, 22)
	store.Tick()

	hist := store.History(1)
	if len(hist) == 0 {
		t.Fatal("expected some history entries after waiting for ticks")
	}
	if hist[0].TotalReqs != 0 || hist[0].PromptTokens != 0 || hist[0].CompletionTokens != 0 {
		t.Fatalf("expected first history entry to be zero baseline, got %+v", hist[0])
	}
	if hist[0].BucketSeconds != 5 {
		t.Fatalf("expected base bucket_seconds of 5, got %d", hist[0].BucketSeconds)
	}
	last := hist[len(hist)-1]
	if last.TotalReqs != 1 {
		t.Fatalf("expected final request delta of 1, got %d", last.TotalReqs)
	}
	if last.PromptTokens != 11 || last.CompletionTokens != 22 {
		t.Fatalf("expected token deltas 11/22, got %d/%d", last.PromptTokens, last.CompletionTokens)
	}

	for i, e := range hist {
		if e.TotalReqs < -1e6 && i > 0 {
			t.Errorf("entry %d: large negative delta %d", i, e.TotalReqs)
		}
		if e.Timestamp.IsZero() {
			t.Errorf("entry %d: zero timestamp", i)
		}
		if e.Label == "" {
			t.Errorf("entry %d: empty label", i)
		}
	}
}

func TestRingStore_Stop(t *testing.T) {
	m := New()
	store := NewRingStore(m, nil)

	store.Stop()

	snap := store.Snapshot()
	if snap["active_connections"] == nil {
		t.Fatal("snapshot should still work after Stop")
	}
}

func TestRingStore_History_ManyMinutes(t *testing.T) {
	m := New()
	store := NewRingStore(m, nil)
	defer store.Stop()

	store.Tick()

	// Large minute value — should not exceed ring buffer size
	hist := store.History(60)
	if len(hist) > 720 {
		t.Fatalf("history exceeds ring buffer size: got %d", len(hist))
	}
}

func TestRingStore_History_ZeroMinutes(t *testing.T) {
	m := New()
	store := NewRingStore(m, nil)
	defer store.Stop()

	hist := store.History(0)
	if len(hist) != 0 {
		t.Fatalf("expected empty history for 0 minutes, got %d", len(hist))
	}
}

func TestRingStore_ConcurrentTicks(t *testing.T) {
	m := New()
	store := NewRingStore(m, nil)
	defer store.Stop()

	for i := 0; i < 20; i++ {
		m.RecordRequest(1*time.Millisecond, false)
		m.IncrementActive()
	}

	snap := store.Snapshot()
	if snap["active_connections"] == nil {
		t.Fatal("snapshot should have active_connections")
	}
	if got := snap["active_connections"].(int64); got != 20 {
		t.Fatalf("active_connections: got %d, want 20", got)
	}
}

func TestRingStore_History_StructuralIntegrity(t *testing.T) {
	m := New()
	store := NewRingStore(m, nil)
	defer store.Stop()

	store.Tick()

	hist := store.History(10)
	if len(hist) == 0 {
		t.Fatal("expected entries after a few ticks")
	}

	for i, e := range hist {
		if e.Timestamp.IsZero() {
			t.Errorf("entry %d has zero timestamp", i)
		}
		if e.Successful < -1e6 && i > 0 {
			t.Errorf("entry %d: unexpected large negative successful_delta %d", i, e.Successful)
		}
	}
}

func TestRingStore_HistorySeries_CompressesToMaxPoints(t *testing.T) {
	m := New()
	tracker := NewModelUsageTracker()
	defer tracker.Stop()
	store := NewRingStore(m, tracker)
	defer store.Stop()

	store.Tick()
	for i := 0; i < 12; i++ {
		m.RecordRequest(1*time.Millisecond, i%2 == 0)
		tracker.Record("qwen", 2, 3)
		store.Tick()
	}

	series := store.HistorySeries(1, 4)
	if len(series) > 4 {
		t.Fatalf("expected compressed series to stay within 4 points, got %d", len(series))
	}
	if len(series) == 0 {
		t.Fatal("expected non-empty compressed series")
	}
	if series[0].BucketSeconds <= 0 {
		t.Fatalf("expected positive bucket_seconds, got %d", series[0].BucketSeconds)
	}

	var reqs, prompt, completion int64
	for _, entry := range series {
		reqs += entry.TotalReqs
		prompt += entry.PromptTokens
		completion += entry.CompletionTokens
	}
	if reqs != 12 {
		t.Fatalf("expected 12 total requests after compression, got %d", reqs)
	}
	if prompt != 24 || completion != 36 {
		t.Fatalf("expected token totals 24/36 after compression, got %d/%d", prompt, completion)
	}
}

func TestRingStore_NewReturnsNotNil(t *testing.T) {
	m := New()
	store := NewRingStore(m, nil)
	if store == nil {
		t.Fatal("NewRingStore returned nil")
	}
	store.Stop()
}
