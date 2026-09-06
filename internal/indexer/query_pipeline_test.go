package indexer

import (
	"strings"
	"testing"
	"time"
)

func TestQueryPipelinePreservesOrderAndDictionaryTerms(t *testing.T) {
	pipeline := NewQueryPipeline()
	query := pipeline.Build("Fabric 模组 1.20.1!!!", DefaultRuntimeConfig().Query)

	joined := strings.Join(query.Terms, "|")
	if !strings.Contains(joined, "fabric|模组|1.20.1") {
		t.Fatalf("expected ordered terms with dictionary word preserved, got %q", joined)
	}
	if len(query.Versions) != 1 || query.Versions[0] != "1.20.1" {
		t.Fatalf("expected version boost token, got %#v", query.Versions)
	}
	if len(query.Loaders) == 0 || query.Loaders[0] != "fabric" {
		t.Fatalf("expected fabric loader detection, got %#v", query.Loaders)
	}
	if !containsString(query.ExpandedTerms, "mod") {
		t.Fatalf("expected 模组 synonym expansion to include mod, got %#v", query.ExpandedTerms)
	}
}

func TestQueryPipelineEmptyAndSymbolsOnly(t *testing.T) {
	pipeline := NewQueryPipeline()
	query := pipeline.Build(" !!! / ", DefaultRuntimeConfig().Query)
	if len(query.Terms) != 0 || query.TSQueryText != "" {
		t.Fatalf("expected empty structured query for symbols only, got %#v", query)
	}
}

func TestCursorCarriesScoredAt(t *testing.T) {
	now := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	cursor := encodeCursor(SearchResult{ID: "resource:1", Score: 1.25, UpdatedAt: now}, now)
	decoded, err := decodeCursor(cursor)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ScoredAt == "" {
		t.Fatal("expected scored_at in cursor")
	}
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}
