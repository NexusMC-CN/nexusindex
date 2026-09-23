package indexer

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

// These cases catch synonym conjunctions, dictionary prefix splitting, and
// different token boundaries between the index and query paths.
func TestQuerySynonymAlternativesAndSharedTokens(t *testing.T) {
	cfg := DefaultRuntimeConfig().Query
	for _, tc := range []struct {
		raw, want string
	}{
		{"模组", "(('模' & '组') | 'mod' | 'mods')"},
		{"modern", "'modern'"},
		{"hello世界", "'hello' & '世' & '界'"},
		{"1.20.1", "'1.20.1'"},
		{"テスト", "'テスト'"},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			if got := NewQueryPipeline().Build(tc.raw, cfg).TSQueryText; got != tc.want {
				t.Fatalf("query %q: got %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestTokenSearchKeepsVersionsAndPhraseOrder(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want []string
	}{
		{"hello世界", []string{"hello", "世", "界"}},
		{"1.20.1", []string{"1.20.1"}},
		{"fabric api fabric", []string{"fabric", "api", "fabric"}},
	} {
		if got := tokenizeForSearch(tc.raw); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("tokenize %q: got %#v, want %#v", tc.raw, got, tc.want)
		}
	}
	if got := ftsText("fabric api fabric"); got != "fabric api fabric" {
		t.Errorf("index text changed phrase positions: %q", got)
	}
}

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
	codec := newCursorCodec("test-secret")
	cursor, err := codec.encode(searchCursor{
		Sort: SortRelevance, Score: 1.25, SortTime: now.Format(time.RFC3339Nano),
		ScoredAt: now.Format(time.RFC3339Nano), GenerationID: 1, ConfigVersion: 1, QueryHash: "query",
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.decode(cursor)
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
