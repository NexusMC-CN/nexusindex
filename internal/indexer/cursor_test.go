package indexer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCursorFromPreviousConfigIsRejectedBeforeDatabaseAccess(t *testing.T) {
	defer func() {
		if value := recover(); value != nil {
			t.Errorf("old cursor reached database instead of config rejection: %v", value)
		}
	}()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	query := SearchQuery{Query: "example", Sort: SortRelevance, Limit: 1}
	query, _ = normalizeSearchQuery(query)
	cursor, err := newCursorCodec("same-secret").encode(searchCursor{Sort: SortRelevance, ScoredAt: now, SortTime: now, GenerationID: 1, ConfigVersion: 1, ConfigHash: newRuntimeConfigStore(DefaultRuntimeConfig()).Snapshot().Hash, QueryHash: searchQueryHash(query)})
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultRuntimeConfig()
	cfg.Scoring.TitleWeight = 42
	restarted := NewServiceWithRuntimeAndCursor(nil, nil, cfg, "same-secret")
	query.Cursor = cursor
	if _, err := restarted.Search(context.Background(), query); !errors.Is(err, ErrCursorMismatch) {
		t.Fatalf("expected config mismatch, got %v", err)
	}
}

func TestSearchSnapshotControlsCursorIdentityAfterConfigPatch(t *testing.T) {
	defer func() {
		if value := recover(); value != nil {
			t.Errorf("new store snapshot accepted cursor instead of captured config rejection: %v", value)
		}
	}()
	service := NewServiceWithRuntimeAndCursor(nil, nil, DefaultRuntimeConfig(), "snapshot-test")
	old := service.RuntimeSnapshot()
	patch := RuntimeConfig{Scoring: ScoringConfig{TitleWeight: 77}}.WithScoringOverride("title_weight")
	service.PatchRuntimeConfig(context.Background(), patch)
	current := service.RuntimeSnapshot()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	query := SearchQuery{Query: "example", Sort: SortRelevance, Limit: 1}
	query, _ = normalizeSearchQuery(query)
	value, err := service.cursor.encode(searchCursor{Sort: SortRelevance, ScoredAt: now, SortTime: now, GenerationID: 1, ConfigVersion: current.Version, ConfigHash: current.Hash, QueryHash: searchQueryHash(query)})
	if err != nil {
		t.Fatal(err)
	}
	query.Cursor = value
	_, searchErr := service.SearchWithSnapshot(context.Background(), query, old)
	if !errors.Is(searchErr, ErrCursorMismatch) {
		t.Fatalf("captured snapshot did not validate cursor identity: %v", searchErr)
	}
}

func TestCursorIntegrationSameConfigRestartAndChangedConfig(t *testing.T) {
	service := searchIntegrationService(t)
	putSearchTestDocument(t, service, SearchDocument{EntityID: "first", Title: "example"})
	putSearchTestDocument(t, service, SearchDocument{EntityID: "second", Title: "example"})
	query := SearchQuery{Query: "example", Sort: SortRelevance, Limit: 1}
	first, err := service.Search(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if first.NextCursor == "" {
		t.Fatal("missing cursor")
	}
	query.Cursor = first.NextCursor
	unchanged := NewServiceWithRuntimeAndCursor(service.mainDB, service.indexDB, service.RuntimeConfig(), "search-test")
	if page, err := unchanged.Search(context.Background(), query); err != nil || len(page.Items) != 1 {
		t.Fatalf("unchanged restart rejected cursor: page=%+v err=%v", page, err)
	}
	cfg := service.RuntimeConfig()
	cfg.Scoring.TitleWeight = 42
	changed := NewServiceWithRuntimeAndCursor(service.mainDB, service.indexDB, cfg, "search-test")
	if _, err := changed.Search(context.Background(), query); !errors.Is(err, ErrCursorMismatch) {
		t.Fatalf("changed restart accepted cursor: %v", err)
	}
}

func TestCursorCodecRejectsTampering(t *testing.T) {
	codec := newCursorCodec("test-secret")
	now := time.Now().UTC().Format(time.RFC3339Nano)
	value, err := codec.encode(searchCursor{
		Sort: SortRelevance, SortTime: now, ScoredAt: now,
		GenerationID: 4, ConfigVersion: 2, QueryHash: "query",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.decode(value); err != nil {
		t.Fatal(err)
	}
	if _, err := codec.decode(strings.Replace(value, "A", "B", 1)); err == nil {
		t.Fatal("expected tampered cursor rejection")
	}
}

func TestSearchQueryHashNormalizesTagOrder(t *testing.T) {
	a := SearchQuery{Query: "fabric", Sort: SortPopular, Tags: []string{"Mod", "fabric"}}
	b := SearchQuery{Query: "fabric", Sort: SortPopular, Tags: []string{"fabric", "mod"}}
	if searchQueryHash(a) != searchQueryHash(b) {
		t.Fatal("expected stable normalized hash")
	}
}
