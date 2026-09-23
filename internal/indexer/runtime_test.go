package indexer

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"testing"
)

func TestRuntimeSnapshotPairsConfigVersionAndHash(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	cfg.Scoring.TitleWeight = 1
	store := newRuntimeConfigStore(cfg)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for version := 2; version <= 500; version++ {
			cfg.Scoring.TitleWeight = float64(version % 101)
			store.Update(context.Background(), cfg)
		}
	}()
	for {
		snapshot := store.Snapshot()
		raw, err := json.Marshal(snapshot.Config)
		if err != nil {
			t.Fatal(err)
		}
		wantHash := fmt.Sprintf("%x", sha256.Sum256(raw))
		if snapshot.Config.Scoring.TitleWeight != float64(snapshot.Version%101) || snapshot.Hash != wantHash {
			t.Fatalf("torn snapshot: version=%d title=%v hash=%s want=%s", snapshot.Version, snapshot.Config.Scoring.TitleWeight, snapshot.Hash, wantHash)
		}
		snapshot.Config.Query.Synonyms["fabric"][0] = "mutated"
		select {
		case <-done:
			if store.Get().Query.Synonyms["fabric"][0] == "mutated" {
				t.Fatal("snapshot mutated store")
			}
			return
		default:
		}
	}
}

func TestRuntimeHashSurvivesRestartAndChangesWithContent(t *testing.T) {
	first := newRuntimeConfigStore(DefaultRuntimeConfig()).Snapshot()
	cfg := DefaultRuntimeConfig().WithScoringOverride("title_weight")
	second := newRuntimeConfigStore(cfg).Snapshot()
	if first.Hash == "" || first.Hash != second.Hash {
		t.Fatal("same effective config has a different identity after restart")
	}
	cfg.Scoring.TitleWeight = 42
	changed := newRuntimeConfigStore(cfg).Snapshot()
	if first.Version != changed.Version || first.Hash == changed.Hash {
		t.Fatal("restart reused configuration identity after content changed")
	}
}

func TestRuntimeConfigDeepCopyAndExplicitZero(t *testing.T) {
	var override RuntimeConfig
	if err := json.Unmarshal([]byte(`{"scoring":{"title_weight":0},"query":{"synonyms":{"模组":["mod"]}}}`), &override); err != nil {
		t.Fatal(err)
	}

	cfg := DefaultRuntimeConfig().Merge(override).Normalized()
	if cfg.Scoring.TitleWeight != 0 {
		t.Fatalf("expected explicit zero title weight, got %v", cfg.Scoring.TitleWeight)
	}

	store := newRuntimeConfigStore(cfg)
	snapshot := store.Get()
	snapshot.Query.Synonyms["模组"][0] = "mutated"

	next := store.Get()
	if next.Query.Synonyms["模组"][0] != "mod" {
		t.Fatalf("runtime config map was mutated through Get snapshot: %#v", next.Query.Synonyms["模组"])
	}
}

func TestRuntimeConfigConcurrentUpdateSnapshots(t *testing.T) {
	store := newRuntimeConfigStore(DefaultRuntimeConfig())
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				cfg := store.Get()
				cfg.Query.Synonyms["fabric"] = []string{"changed"}
				override := RuntimeConfig{}
				override.Scoring.TitleWeight = float64(i % 7)
				override = override.WithScoringOverride("title_weight")
				store.Update(context.Background(), store.Get().Merge(override))
			}
		}(i)
	}
	wg.Wait()

	cfg := store.Get()
	if cfg.Query.Synonyms["fabric"][0] == "changed" {
		t.Fatal("stored synonyms were mutated by concurrent reader snapshot")
	}
}

func TestRuntimeConfigClampInvalidValues(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	cfg.Scoring.TitleWeight = math.Inf(1)
	cfg.Scoring.TagsWeight = -10
	cfg.Scoring.TimeDecayFactor = math.NaN()
	cfg.CandidateLimit = 1_000_000

	normalized := cfg.Normalized()
	if normalized.Scoring.TitleWeight != DefaultRuntimeConfig().Scoring.TitleWeight {
		t.Fatalf("expected inf title weight fallback, got %v", normalized.Scoring.TitleWeight)
	}
	if normalized.Scoring.TagsWeight != 0 {
		t.Fatalf("expected negative tags weight clamp to 0, got %v", normalized.Scoring.TagsWeight)
	}
	if normalized.Scoring.TimeDecayFactor != DefaultRuntimeConfig().Scoring.TimeDecayFactor {
		t.Fatalf("expected nan time decay fallback, got %v", normalized.Scoring.TimeDecayFactor)
	}
	if normalized.CandidateLimit != 5000 {
		t.Fatalf("expected candidate limit clamp to 5000, got %d", normalized.CandidateLimit)
	}
}
