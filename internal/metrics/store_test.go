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
	store := NewRingStore(m, nil)
	defer store.Stop()

	// Let a few ticks happen
	time.Sleep(12 * time.Second)

	m.RecordRequest(10*time.Millisecond, false)

	hist := store.History(1)
	if len(hist) == 0 {
		t.Fatal("expected some history entries after waiting for ticks")
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

	time.Sleep(12 * time.Second)

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
}

func TestRingStore_History_StructuralIntegrity(t *testing.T) {
	m := New()
	store := NewRingStore(m, nil)
	defer store.Stop()

	time.Sleep(8 * time.Second)

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

func TestRingStore_NewReturnsNotNil(t *testing.T) {
	m := New()
	store := NewRingStore(m, nil)
	if store == nil {
		t.Fatal("NewRingStore returned nil")
	}
	store.Stop()
}
