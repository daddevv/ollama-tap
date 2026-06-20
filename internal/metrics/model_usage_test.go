package metrics

import (
	"sync"
	"testing"
)

func TestModelUsageTracker_Record(t *testing.T) {
	tracker := NewModelUsageTracker()

	tracker.Record("gpt-4", 100, 200)
	snap := tracker.Snapshot()

	u, ok := snap["gpt-4"]
	if !ok {
		t.Fatal("missing gpt-4 in snapshot")
	}
	if u.PromptTokens != 100 {
		t.Errorf("prompt_tokens: got %d, want 100", u.PromptTokens)
	}
	if u.CompletionTokens != 200 {
		t.Errorf("completion_tokens: got %d, want 200", u.CompletionTokens)
	}
	if u.TotalTokens != 300 {
		t.Errorf("total_tokens: got %d, want 300", u.TotalTokens)
	}
	if u.RequestCount != 1 {
		t.Errorf("request_count: got %d, want 1", u.RequestCount)
	}
}

func TestModelUsageTracker_Aggregation(t *testing.T) {
	tracker := NewModelUsageTracker()

	tracker.Record("llama3", 50, 100)
	tracker.Record("llama3", 75, 150)
	snap := tracker.Snapshot()

	u := snap["llama3"]
	if u.RequestCount != 2 {
		t.Errorf("request_count: got %d, want 2", u.RequestCount)
	}
	if u.PromptTokens != 125 {
		t.Errorf("prompt_tokens: got %d, want 125", u.PromptTokens)
	}
	if u.CompletionTokens != 250 {
		t.Errorf("completion_tokens: got %d, want 250", u.CompletionTokens)
	}
	if u.TotalTokens != 375 {
		t.Errorf("total_tokens: got %d, want 375", u.TotalTokens)
	}
}

func TestModelUsageTracker_EmptyModel(t *testing.T) {
	tracker := NewModelUsageTracker()
	tracker.Record("", 100, 200) // empty model name should be ignored

	if tracker.HasRecords() {
		t.Fatal("expected no records for empty model name")
	}
}

func TestModelUsageTracker_Concurrent(t *testing.T) {
	tracker := NewModelUsageTracker()
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			model := "model"
			if id%2 == 0 {
				model = "gpt-4"
			}
			for j := 0; j < 1000; j++ {
				tracker.Record(model, 1, 1)
			}
		}(i)
	}

	wg.Wait()

	snap := tracker.Snapshot()
	gpt4 := snap["gpt-4"]
	other := snap["model"]

	if gpt4.PromptTokens != 5000 {
		t.Errorf("gpt-4 prompt_tokens: got %d, want 5000", gpt4.PromptTokens)
	}
	if other.PromptTokens != 5000 {
		t.Errorf("model prompt_tokens: got %d, want 5000", other.PromptTokens)
	}
}

func TestModelUsageTracker_MultipleModels(t *testing.T) {
	tracker := NewModelUsageTracker()

	tracker.Record("model-a", 10, 20)
	tracker.Record("model-b", 30, 60)
	tracker.Record("model-c", 5, 10)

	snap := tracker.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("expected 3 models in snapshot, got %d", len(snap))
	}
	for name, u := range snap {
		if u.TotalTokens == 0 {
			t.Errorf("model %s has zero total_tokens", name)
		}
	}
}

func TestModelUsageTracker_NewIsNotNil(t *testing.T) {
	tracker := NewModelUsageTracker()
	if tracker == nil {
		t.Fatal("NewModelUsageTracker returned nil")
	}
	if tracker.HasRecords() {
		// HasRecords should return false for empty, which is fine
	}
}

func TestModelUsageTracker_SnapshotReturnsCopy(t *testing.T) {
	tracker := NewModelUsageTracker()
	tracker.Record("test", 100, 200)

	snap1 := tracker.Snapshot()
	tracker.Record("test", 50, 100)
	snap2 := tracker.Snapshot()

	if snap1["test"].TotalTokens != 300 {
		t.Errorf("snap1 total_tokens should be 300, got %d", snap1["test"].TotalTokens)
	}
	if snap2["test"].TotalTokens != 450 {
		t.Errorf("snap2 total_tokens should be 450, got %d", snap2["test"].TotalTokens)
	}
}

func TestModelUsageTracker_RecordRequestAndTokensSeparately(t *testing.T) {
	tracker := NewModelUsageTracker()

	tracker.RecordRequest("qwen")
	tracker.RecordTokens("qwen", 12, 34)

	snap := tracker.Snapshot()
	u := snap["qwen"]
	if u.RequestCount != 1 {
		t.Fatalf("request_count: got %d, want 1", u.RequestCount)
	}
	if u.PromptTokens != 12 {
		t.Fatalf("prompt_tokens: got %d, want 12", u.PromptTokens)
	}
	if u.CompletionTokens != 34 {
		t.Fatalf("completion_tokens: got %d, want 34", u.CompletionTokens)
	}
	if u.TotalTokens != 46 {
		t.Fatalf("total_tokens: got %d, want 46", u.TotalTokens)
	}
}
