package indexer

import (
	"testing"
	"time"
)

func TestMergeLatestEventsByDocID(t *testing.T) {
	base := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	events := []IndexEvent{
		{ID: "1", EntityType: EntityResource, EntityID: "r1", Action: "upsert", EventOrder: 1, CreatedAt: base},
		{ID: "2", EntityType: EntityResource, EntityID: "r1", Action: "delete", EventOrder: 2, CreatedAt: base.Add(time.Second)},
		{ID: "3", EntityType: EntityPost, EntityID: "p1", Action: "upsert", EventOrder: 3, CreatedAt: base},
	}

	merged := mergeLatestEvents(events)
	if len(merged) != 2 {
		t.Fatalf("expected 2 merged docs, got %d", len(merged))
	}
	if merged[EntityResource+":r1"].Action != "delete" {
		t.Fatalf("expected latest resource action delete, got %#v", merged[EntityResource+":r1"])
	}
	if merged[EntityPost+":p1"].Action != "upsert" {
		t.Fatalf("expected post action upsert, got %#v", merged[EntityPost+":p1"])
	}
}

func TestMergeLatestEventsPrefersEventOrderOverTimestamp(t *testing.T) {
	base := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	events := []IndexEvent{
		{ID: "newer-time", EntityType: EntityPost, EntityID: "p1", Action: "upsert", EventOrder: 10, CreatedAt: base.Add(time.Minute)},
		{ID: "newer-order", EntityType: EntityPost, EntityID: "p1", Action: "delete", EventOrder: 11, CreatedAt: base},
	}

	merged := mergeLatestEvents(events)
	if merged[EntityPost+":p1"].ID != "newer-order" {
		t.Fatalf("expected event order to win, got %#v", merged[EntityPost+":p1"])
	}
}

func TestNewWorkerAppliesSafeDefaults(t *testing.T) {
	worker := NewWorker(nil, WorkerOptions{})

	if worker.options.BatchSize != 200 {
		t.Fatalf("expected default batch size 200, got %d", worker.options.BatchSize)
	}
	if worker.options.PollInterval != 2*time.Second {
		t.Fatalf("expected default poll interval 2s, got %s", worker.options.PollInterval)
	}
	if worker.options.LockTTL != 2*time.Minute {
		t.Fatalf("expected default lock TTL 2m, got %s", worker.options.LockTTL)
	}
	if worker.options.RetryDelay != 30*time.Second {
		t.Fatalf("expected default retry delay 30s, got %s", worker.options.RetryDelay)
	}
	if worker.options.MaxAttempts != 10 {
		t.Fatalf("expected default max attempts 10, got %d", worker.options.MaxAttempts)
	}
}

func TestNewWorkerCapsBatchSize(t *testing.T) {
	worker := NewWorker(nil, WorkerOptions{BatchSize: 1000})
	if worker.options.BatchSize != 500 {
		t.Fatalf("expected capped batch size 500, got %d", worker.options.BatchSize)
	}
}
