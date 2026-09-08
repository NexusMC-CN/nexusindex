package indexer

import (
	"strings"
	"testing"
	"time"
)

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
